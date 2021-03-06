import datetime
import errno
import json
import yaml

import six

from ceph.deployment.inventory import Device
from prettytable import PrettyTable

from mgr_util import format_bytes, to_pretty_timedelta

try:
    from typing import List, Set, Optional, Dict
except ImportError:
    pass  # just for type checking.


from ceph.deployment.drive_group import DriveGroupSpec, DeviceSelection, \
    DriveGroupSpecs
from mgr_module import MgrModule, HandleCommandResult

from ._interface import OrchestratorClientMixin, DeviceLightLoc, _cli_read_command, \
    raise_if_exception, _cli_write_command, TrivialReadCompletion, OrchestratorError, \
    NoOrchestrator, ServiceSpec, PlacementSpec, OrchestratorValidationError, NFSServiceSpec, \
    RGWSpec, InventoryFilter, InventoryNode, HostPlacementSpec, HostSpec, CLICommandMeta


@six.add_metaclass(CLICommandMeta)
class OrchestratorCli(OrchestratorClientMixin, MgrModule):
    MODULE_OPTIONS = [
        {
            'name': 'orchestrator',
            'type': 'str',
            'default': None,
            'desc': 'Orchestrator backend',
            'enum_allowed': ['cephadm', 'rook',
                             'test_orchestrator'],
            'runtime': True,
        },
    ]
    NATIVE_OPTIONS = []  # type: List[dict]

    def __init__(self, *args, **kwargs):
        super(OrchestratorCli, self).__init__(*args, **kwargs)
        self.ident = set()  # type: Set[str]
        self.fault = set()  # type: Set[str]
        self._load()
        self._refresh_health()

    def _load(self):
        active = self.get_store('active_devices')
        if active:
            decoded = json.loads(active)
            self.ident = set(decoded.get('ident', []))
            self.fault = set(decoded.get('fault', []))
        self.log.debug('ident {}, fault {}'.format(self.ident, self.fault))

    def _save(self):
        encoded = json.dumps({
            'ident': list(self.ident),
            'fault': list(self.fault),
            })
        self.set_store('active_devices', encoded)

    def _refresh_health(self):
        h = {}
        if self.ident:
            h['DEVICE_IDENT_ON'] = {
                'severity': 'warning',
                'summary': '%d devices have ident light turned on' % len(
                    self.ident),
                'detail': ['{} ident light enabled'.format(d) for d in self.ident]
            }
        if self.fault:
            h['DEVICE_FAULT_ON'] = {
                'severity': 'warning',
                'summary': '%d devices have fault light turned on' % len(
                    self.fault),
                'detail': ['{} fault light enabled'.format(d) for d in self.ident]
            }
        self.set_health_checks(h)

    def _get_device_locations(self, dev_id):
        # type: (str) -> List[DeviceLightLoc]
        locs = [d['location'] for d in self.get('devices')['devices'] if d['devid'] == dev_id]
        return [DeviceLightLoc(**l) for l in sum(locs, [])]

    @_cli_read_command(
        prefix='device ls-lights',
        desc='List currently active device indicator lights')
    def _device_ls(self):
        return HandleCommandResult(
            stdout=json.dumps({
                'ident': list(self.ident),
                'fault': list(self.fault)
                }, indent=4, sort_keys=True))

    def light_on(self, fault_ident, devid):
        # type: (str, str) -> HandleCommandResult
        assert fault_ident in ("fault", "ident")
        locs = self._get_device_locations(devid)
        if locs is None:
            return HandleCommandResult(stderr='device {} not found'.format(devid),
                                       retval=-errno.ENOENT)

        getattr(self, fault_ident).add(devid)
        self._save()
        self._refresh_health()
        completion = self.blink_device_light(fault_ident, True, locs)
        self._orchestrator_wait([completion])
        return HandleCommandResult(stdout=str(completion.result))

    def light_off(self, fault_ident, devid, force):
        # type: (str, str, bool) -> HandleCommandResult
        assert fault_ident in ("fault", "ident")
        locs = self._get_device_locations(devid)
        if locs is None:
            return HandleCommandResult(stderr='device {} not found'.format(devid),
                                       retval=-errno.ENOENT)

        try:
            completion = self.blink_device_light(fault_ident, False, locs)
            self._orchestrator_wait([completion])

            if devid in getattr(self, fault_ident):
                getattr(self, fault_ident).remove(devid)
                self._save()
                self._refresh_health()
            return HandleCommandResult(stdout=str(completion.result))

        except:
            # There are several reasons the try: block might fail:
            # 1. the device no longer exist
            # 2. the device is no longer known to Ceph
            # 3. the host is not reachable
            if force and devid in getattr(self, fault_ident):
                getattr(self, fault_ident).remove(devid)
                self._save()
                self._refresh_health()
            raise

    @_cli_write_command(
        prefix='device light',
        cmd_args='name=enable,type=CephChoices,strings=on|off '
                 'name=devid,type=CephString '
                 'name=light_type,type=CephChoices,strings=ident|fault,req=false '
                 'name=force,type=CephBool,req=false',
        desc='Enable or disable the device light. Default type is `ident`\n'
             'Usage: device light (on|off) <devid> [ident|fault] [--force]')
    def _device_light(self, enable, devid, light_type=None, force=False):
        # type: (str, str, Optional[str], bool) -> HandleCommandResult
        light_type = light_type or 'ident'
        on = enable == 'on'
        if on:
            return self.light_on(light_type, devid)
        else:
            return self.light_off(light_type, devid, force)

    def _select_orchestrator(self):
        return self.get_module_option("orchestrator")

    @_cli_write_command(
        'orch host add',
        'name=host,type=CephString,req=true '
        'name=addr,type=CephString,req=false '
        'name=labels,type=CephString,n=N,req=false',
        'Add a host')
    def _add_host(self, host, addr=None, labels=None):
        s = HostSpec(hostname=host, addr=addr, labels=labels)
        completion = self.add_host(s)
        self._orchestrator_wait([completion])
        raise_if_exception(completion)
        return HandleCommandResult(stdout=completion.result_str())

    @_cli_write_command(
        'orch host rm',
        "name=host,type=CephString,req=true",
        'Remove a host')
    def _remove_host(self, host):
        completion = self.remove_host(host)
        self._orchestrator_wait([completion])
        raise_if_exception(completion)
        return HandleCommandResult(stdout=completion.result_str())

    @_cli_write_command(
        'orch host set-addr',
        'name=host,type=CephString '
        'name=addr,type=CephString',
        'Update a host address')
    def _update_set_addr(self, host, addr):
        completion = self.update_host_addr(host, addr)
        self._orchestrator_wait([completion])
        raise_if_exception(completion)
        return HandleCommandResult(stdout=completion.result_str())

    @_cli_read_command(
        'orch host ls',
        'name=format,type=CephChoices,strings=json|plain,req=false',
        'List hosts')
    def _get_hosts(self, format='plain'):
        completion = self.get_hosts()
        self._orchestrator_wait([completion])
        raise_if_exception(completion)
        if format == 'json':
            hosts = [dict(host=node.name, labels=node.labels)
                     for node in completion.result]
            output = json.dumps(hosts, sort_keys=True)
        else:
            table = PrettyTable(
                ['HOST', 'ADDR', 'LABELS'],
                border=False)
            table.align = 'l'
            table.left_padding_width = 0
            table.right_padding_width = 1
            for node in completion.result:
                table.add_row((node.name, node.addr, ' '.join(node.labels)))
            output = table.get_string()
        return HandleCommandResult(stdout=output)

    @_cli_write_command(
        'orch host label add',
        'name=host,type=CephString '
        'name=label,type=CephString',
        'Add a host label')
    def _host_label_add(self, host, label):
        completion = self.add_host_label(host, label)
        self._orchestrator_wait([completion])
        raise_if_exception(completion)
        return HandleCommandResult(stdout=completion.result_str())

    @_cli_write_command(
        'orch host label rm',
        'name=host,type=CephString '
        'name=label,type=CephString',
        'Add a host label')
    def _host_label_rm(self, host, label):
        completion = self.remove_host_label(host, label)
        self._orchestrator_wait([completion])
        raise_if_exception(completion)
        return HandleCommandResult(stdout=completion.result_str())

    @_cli_read_command(
        'orch device ls',
        "name=host,type=CephString,n=N,req=false "
        "name=format,type=CephChoices,strings=json|plain,req=false "
        "name=refresh,type=CephBool,req=false",
        'List devices on a node')
    def _list_devices(self, host=None, format='plain', refresh=False):
        # type: (Optional[List[str]], str, bool) -> HandleCommandResult
        """
        Provide information about storage devices present in cluster hosts

        Note: this does not have to be completely synchronous. Slightly out of
        date hardware inventory is fine as long as hardware ultimately appears
        in the output of this command.
        """
        nf = InventoryFilter(nodes=host) if host else None

        completion = self.get_inventory(node_filter=nf, refresh=refresh)

        self._orchestrator_wait([completion])
        raise_if_exception(completion)

        if format == 'json':
            data = [n.to_json() for n in completion.result]
            return HandleCommandResult(stdout=json.dumps(data))
        else:
            out = []

            table = PrettyTable(
                ['HOST', 'PATH', 'TYPE', 'SIZE', 'DEVICE', 'AVAIL',
                 'REJECT REASONS'],
                border=False)
            table.align = 'l'
            table._align['SIZE'] = 'r'
            table.left_padding_width = 0
            table.right_padding_width = 1
            for host_ in completion.result: # type: InventoryNode
                for d in host_.devices.devices:  # type: Device
                    table.add_row(
                        (
                            host_.name,
                            d.path,
                            d.human_readable_type,
                            format_bytes(d.sys_api.get('size', 0), 5),
                            d.device_id,
                            d.available,
                            ', '.join(d.rejected_reasons)
                        )
                    )
            out.append(table.get_string())
            return HandleCommandResult(stdout='\n'.join(out))

    @_cli_read_command(
        'orch ps',
        "name=host,type=CephString,req=false "
        "name=daemon_type,type=CephChoices,strings=mon|mgr|osd|mds|iscsi|nfs|rgw|rbd-mirror,req=false "
        "name=daemon_id,type=CephString,req=false "
        "name=format,type=CephChoices,strings=json|plain,req=false "
        "name=refresh,type=CephBool,req=false",
        'List daemons known to orchestrator')
    def _list_daemons(self, host=None, daemon_type=None, daemon_id=None, format='plain', refresh=False):
        completion = self.list_daemons(daemon_type,
                                       daemon_id=daemon_id,
                                       host=host,
                                       refresh=refresh)
        self._orchestrator_wait([completion])
        raise_if_exception(completion)
        daemons = completion.result

        def ukn(s):
            return '<unknown>' if s is None else s
        # Sort the list for display
        daemons.sort(key=lambda s: (ukn(s.daemon_type), ukn(s.nodename), ukn(s.daemon_id)))

        if len(daemons) == 0:
            return HandleCommandResult(stdout="No daemons reported")
        elif format == 'json':
            data = [s.to_json() for s in daemons]
            return HandleCommandResult(stdout=json.dumps(data))
        else:
            now = datetime.datetime.utcnow()
            table = PrettyTable(
                ['NAME', 'HOST', 'STATUS', 'REFRESHED',
                 'VERSION', 'IMAGE NAME', 'IMAGE ID', 'CONTAINER ID'],
                border=False)
            table.align = 'l'
            table.left_padding_width = 0
            table.right_padding_width = 1
            for s in sorted(daemons, key=lambda s: s.name()):
                status = {
                    -1: 'error',
                    0: 'stopped',
                    1: 'running',
                    None: '<unknown>'
                }[s.status]

                if s.last_refresh:
                    age = to_pretty_timedelta(now - s.last_refresh) + ' ago'
                else:
                    age = '-'
                table.add_row((
                    s.name(),
                    ukn(s.nodename),
                    status,
                    age,
                    ukn(s.version),
                    ukn(s.container_image_name),
                    ukn(s.container_image_id)[0:12],
                    ukn(s.container_id)[0:12]))

            return HandleCommandResult(stdout=table.get_string())

    @_cli_write_command(
        'orch osd create',
        "name=svc_arg,type=CephString,req=false",
        'Create an OSD service. Either --svc_arg=host:drives or -i <drive_group>')
    def _create_osd(self, svc_arg=None, inbuf=None):
        # type: (Optional[str], Optional[str]) -> HandleCommandResult
        """Create one or more OSDs"""

        usage = """
Usage:
  ceph orch osd create -i <json_file/yaml_file>
  ceph orch osd create host:device1,device2,...
"""

        if inbuf:
            try:
                dgs = DriveGroupSpecs(yaml.load(inbuf))
                drive_groups = dgs.drive_groups
            except ValueError as e:
                msg = 'Failed to read JSON input: {}'.format(str(e)) + usage
                return HandleCommandResult(-errno.EINVAL, stderr=msg)

        elif svc_arg:
            try:
                node_name, block_device = svc_arg.split(":")
                block_devices = block_device.split(',')
            except (TypeError, KeyError, ValueError):
                msg = "Invalid host:device spec: '{}'".format(svc_arg) + usage
                return HandleCommandResult(-errno.EINVAL, stderr=msg)

            devs = DeviceSelection(paths=block_devices)
            drive_groups = [DriveGroupSpec(node_name, data_devices=devs)]
        else:
            return HandleCommandResult(-errno.EINVAL, stderr=usage)

        completion = self.create_osds(drive_groups)
        self._orchestrator_wait([completion])
        raise_if_exception(completion)
        return HandleCommandResult(stdout=completion.result_str())

    @_cli_write_command(
        'orch daemon add mon',
        "name=num,type=CephInt,req=false "
        "name=hosts,type=CephString,n=N,req=false "
        "name=label,type=CephString,req=false",
        'Start monitor daemon(s)')
    def _daemon_add_mon(self, num=None, hosts=[], label=None):
        if not num and not hosts and not label:
            # Improve Error message. Point to parse_host_spec examples
            raise OrchestratorValidationError("Mons need a placement spec. (num, host, network, name(opt))")
        placement = PlacementSpec(label=label, count=num, hosts=hosts)
        placement.validate()

        spec = ServiceSpec(placement=placement)

        completion = self.add_mon(spec)
        self._orchestrator_wait([completion])
        raise_if_exception(completion)
        return HandleCommandResult(stdout=completion.result_str())

    @_cli_write_command(
        'orch daemon add mgr',
        "name=num,type=CephInt,req=false "
        "name=hosts,type=CephString,n=N,req=false",
        'Start rbd-mirror daemon(s)')
    def _daemon_add_mgr(self, num=None, hosts=None):
        spec = ServiceSpec(
            placement=PlacementSpec(hosts=hosts, count=num))
        completion = self.add_mgr(spec)
        self._orchestrator_wait([completion])
        raise_if_exception(completion)
        return HandleCommandResult(stdout=completion.result_str())

    @_cli_write_command(
        'orch daemon add rbd-mirror',
        "name=num,type=CephInt,req=false "
        "name=hosts,type=CephString,n=N,req=false",
        'Start rbd-mirror daemon(s)')
    def _rbd_mirror_add(self, num=None, hosts=None):
        spec = ServiceSpec(
            None,
            placement=PlacementSpec(hosts=hosts, count=num))
        completion = self.add_rbd_mirror(spec)
        self._orchestrator_wait([completion])
        raise_if_exception(completion)
        return HandleCommandResult(stdout=completion.result_str())

    @_cli_write_command(
        'orch daemon add mds',
        "name=fs_name,type=CephString "
        "name=num,type=CephInt,req=false "
        "name=hosts,type=CephString,n=N,req=false",
        'Start MDS daemon(s)')
    def _mds_add(self, fs_name, num=None, hosts=None):
        spec = ServiceSpec(
            fs_name,
            placement=PlacementSpec(hosts=hosts, count=num))
        completion = self.add_mds(spec)
        self._orchestrator_wait([completion])
        raise_if_exception(completion)
        return HandleCommandResult(stdout=completion.result_str())

    @_cli_write_command(
        'orch daemon add rgw',
        'name=realm_name,type=CephString '
        'name=zone_name,type=CephString '
        'name=num,type=CephInt,req=false '
        "name=hosts,type=CephString,n=N,req=false",
        'Start RGW daemon(s)')
    def _rgw_add(self, realm_name, zone_name, num=1, hosts=None, inbuf=None):
        usage = """
Usage:
  ceph orch rgw add -i <json_file>
  ceph orch rgw add <realm_name> <zone_name>
        """
        if inbuf:
            try:
                rgw_spec = RGWSpec.from_json(json.loads(inbuf))
            except ValueError as e:
                msg = 'Failed to read JSON input: {}'.format(str(e)) + usage
                return HandleCommandResult(-errno.EINVAL, stderr=msg)
        rgw_spec = RGWSpec(
            rgw_realm=realm_name,
            rgw_zone=zone_name,
            placement=PlacementSpec(hosts=hosts, count=num))

        completion = self.add_rgw(rgw_spec)
        self._orchestrator_wait([completion])
        raise_if_exception(completion)
        return HandleCommandResult(stdout=completion.result_str())

    @_cli_write_command(
        'orch daemon add nfs',
        "name=svc_arg,type=CephString "
        "name=pool,type=CephString "
        "name=namespace,type=CephString,req=false "
        'name=num,type=CephInt,req=false '
        'name=hosts,type=CephString,n=N,req=false '
        'name=label,type=CephString,req=false',
        'Start NFS daemon(s)')
    def _nfs_add(self, svc_arg, pool, namespace=None, num=None, label=None, hosts=[]):
        spec = NFSServiceSpec(
            svc_arg,
            pool=pool,
            namespace=namespace,
            placement=PlacementSpec(label=label, hosts=hosts, count=num),
        )
        spec.validate_add()
        completion = self.add_nfs(spec)
        self._orchestrator_wait([completion])
        raise_if_exception(completion)
        return HandleCommandResult(stdout=completion.result_str())

    @_cli_write_command(
        'orch daemon add prometheus',
        'name=num,type=CephInt,req=false '
        'name=hosts,type=CephString,n=N,req=false '
        'name=label,type=CephString,req=false',
        'Add prometheus daemon(s)')
    def _daemon_add_prometheus(self, num=None, label=None, hosts=[]):
        # type: (Optional[int], Optional[str], List[str]) -> HandleCommandResult
        spec = ServiceSpec(
            placement=PlacementSpec(label=label, hosts=hosts, count=num),
        )
        completion = self.add_prometheus(spec)
        self._orchestrator_wait([completion])
        return HandleCommandResult(stdout=completion.result_str())

    @_cli_write_command(
        'orch',
        "name=action,type=CephChoices,strings=start|stop|restart|redeploy|reconfig "
        "name=svc_name,type=CephString",
        'Start, stop, restart, redeploy, or reconfig an entire service (i.e. all daemons)')
    def _service_action(self, action, svc_name):
        if '.' in svc_name:
            (service_type, service_id) = svc_name.split('.', 1)
        else:
            service_type = svc_name;
            service_id = None
        completion = self.service_action(action, service_type, service_id)
        self._orchestrator_wait([completion])
        raise_if_exception(completion)
        return HandleCommandResult(stdout=completion.result_str())

    @_cli_write_command(
        'orch daemon',
        "name=action,type=CephChoices,strings=start|stop|restart|redeploy|reconfig "
        "name=name,type=CephString",
        'Start, stop, restart, redeploy, or reconfig a specific daemon')
    def _daemon_action(self, action, name):
        if '.' not in name:
            raise OrchestratorError('%s is not a valid daemon name' % name)
        (daemon_type, daemon_id) = name.split('.', 1)
        completion = self.daemon_action(action, daemon_type, daemon_id)
        self._orchestrator_wait([completion])
        raise_if_exception(completion)
        return HandleCommandResult(stdout=completion.result_str())

    @_cli_write_command(
        'orch daemon rm',
        "name=names,type=CephString,n=N "
        'name=force,type=CephBool,req=false',
        'Remove specific daemon(s)')
    def _daemon_rm(self, names, force=False):
        for name in names:
            if '.' not in name:
                raise OrchestratorError('%s is not a valid daemon name' % name)
        completion = self.remove_daemons(names, force)
        self._orchestrator_wait([completion])
        raise_if_exception(completion)
        return HandleCommandResult(stdout=completion.result_str())

    @_cli_write_command(
        'orch rm',
        "name=name,type=CephString",
        'Remove a service')
    def _service_rm(self, name):
        if '.' in name:
            (service_type, service_name) = name.split('.')
        else:
            service_type = name;
            service_name = None
        if name in ['mon', 'mgr']:
            raise OrchestratorError('The mon and mgr services cannot be removed')
        completion = self.remove_service(service_type, service_name)
        self._orchestrator_wait([completion])
        raise_if_exception(completion)
        return HandleCommandResult(stdout=completion.result_str())

    @_cli_write_command(
        'orch apply mgr',
        "name=num,type=CephInt,req=false "
        "name=hosts,type=CephString,n=N,req=false "
        "name=label,type=CephString,req=false",
        'Update the size or placement of managers')
    def _apply_mgr(self, num=None, hosts=[], label=None):
        placement = PlacementSpec(
            label=label, count=num, hosts=hosts)
        placement.validate()

        spec = ServiceSpec(placement=placement)

        completion = self.apply_mgr(spec)
        self._orchestrator_wait([completion])
        raise_if_exception(completion)
        return HandleCommandResult(stdout=completion.result_str())

    @_cli_write_command(
        'orch apply mon',
        "name=num,type=CephInt,req=false "
        "name=hosts,type=CephString,n=N,req=false "
        "name=label,type=CephString,req=false",
        'Update the number of monitor instances')
    def _apply_mon(self, num=None, hosts=[], label=None):
        if not num and not hosts and not label:
            # Improve Error message. Point to parse_host_spec examples
            raise OrchestratorValidationError("Mons need a placement spec. (num, host, network, name(opt))")
        placement = PlacementSpec(label=label, count=num, hosts=hosts)
        placement.validate()

        spec = ServiceSpec(placement=placement)

        completion = self.apply_mon(spec)
        self._orchestrator_wait([completion])
        raise_if_exception(completion)
        return HandleCommandResult(stdout=completion.result_str())

    @_cli_write_command(
        'orch apply mds',
        "name=fs_name,type=CephString "
        "name=num,type=CephInt,req=false "
        "name=hosts,type=CephString,n=N,req=false "
        "name=label,type=CephString,req=false",
        'Update the number of MDS instances for the given fs_name')
    def _apply_mds(self, fs_name, num=None, label=None, hosts=[]):
        placement = PlacementSpec(label=label, count=num, hosts=hosts)
        placement.validate()

        spec = ServiceSpec(
            fs_name,
            placement=placement)

        completion = self.apply_mds(spec)
        self._orchestrator_wait([completion])
        raise_if_exception(completion)
        return HandleCommandResult(stdout=completion.result_str())

    @_cli_write_command(
        'orch apply rbd-mirror',
        "name=num,type=CephInt,req=false "
        "name=hosts,type=CephString,n=N,req=false "
        "name=label,type=CephString,req=false",
        'Update the number of rbd-mirror instances')
    def _apply_rbd_mirror(self, num, label=None, hosts=[]):
        spec = ServiceSpec(
            placement=PlacementSpec(hosts=hosts, count=num, label=label))
        completion = self.apply_rbd_mirror(spec)
        self._orchestrator_wait([completion])
        raise_if_exception(completion)
        return HandleCommandResult(stdout=completion.result_str())

    @_cli_write_command(
        'orch apply rgw',
        'name=realm_name,type=CephString '
        'name=zone_name,type=CephString '
        'name=num,type=CephInt,req=false '
        'name=hosts,type=CephString,n=N,req=false '
        'name=label,type=CephString,req=false',
        'Update the number of RGW instances for the given zone')
    def _apply_rgw(self, zone_name, realm_name, num=None, label=None, hosts=[]):
        spec = RGWSpec(
            rgw_realm=realm_name,
            rgw_zone=zone_name,
            placement=PlacementSpec(hosts=hosts, label=label, count=num))
        completion = self.apply_rgw(spec)
        self._orchestrator_wait([completion])
        raise_if_exception(completion)
        return HandleCommandResult(stdout=completion.result_str())

    @_cli_write_command(
        'orch apply nfs',
        "name=svc_id,type=CephString "
        'name=num,type=CephInt,req=false '
        'name=hosts,type=CephString,n=N,req=false '
        'name=label,type=CephString,req=false',
        'Scale an NFS service')
    def _apply_nfs(self, svc_id, num=None, label=None, hosts=[]):
        # type: (str, Optional[int], Optional[str], List[str]) -> HandleCommandResult
        spec = NFSServiceSpec(
            svc_id,
            placement=PlacementSpec(label=label, hosts=hosts, count=num),
        )
        completion = self.apply_nfs(spec)
        self._orchestrator_wait([completion])
        return HandleCommandResult(stdout=completion.result_str())

    @_cli_write_command(
        'orch apply prometheus',
        'name=num,type=CephInt,req=false '
        'name=hosts,type=CephString,n=N,req=false '
        'name=label,type=CephString,req=false',
        'Scale prometheus service')
    def _apply_prometheus(self, num=None, label=None, hosts=[]):
        # type: (Optional[int], Optional[str], List[str]) -> HandleCommandResult
        spec = ServiceSpec(
            placement=PlacementSpec(label=label, hosts=hosts, count=num),
        )
        completion = self.apply_prometheus(spec)
        self._orchestrator_wait([completion])
        return HandleCommandResult(stdout=completion.result_str())

    @_cli_write_command(
        'orch set backend',
        "name=module_name,type=CephString,req=true",
        'Select orchestrator module backend')
    def _set_backend(self, module_name):
        """
        We implement a setter command instead of just having the user
        modify the setting directly, so that we can validate they're setting
        it to a module that really exists and is enabled.

        There isn't a mechanism for ensuring they don't *disable* the module
        later, but this is better than nothing.
        """
        mgr_map = self.get("mgr_map")

        if module_name is None or module_name == "":
            self.set_module_option("orchestrator", None)
            return HandleCommandResult()

        for module in mgr_map['available_modules']:
            if module['name'] != module_name:
                continue

            if not module['can_run']:
                continue

            enabled = module['name'] in mgr_map['modules']
            if not enabled:
                return HandleCommandResult(-errno.EINVAL,
                                           stderr="Module '{module_name}' is not enabled. \n Run "
                                                  "`ceph mgr module enable {module_name}` "
                                                  "to enable.".format(module_name=module_name))

            try:
                is_orchestrator = self.remote(module_name,
                                              "is_orchestrator_module")
            except NameError:
                is_orchestrator = False

            if not is_orchestrator:
                return HandleCommandResult(-errno.EINVAL,
                                           stderr="'{0}' is not an orchestrator module".format(module_name))

            self.set_module_option("orchestrator", module_name)

            return HandleCommandResult()

        return HandleCommandResult(-errno.EINVAL, stderr="Module '{0}' not found".format(module_name))

    @_cli_write_command(
        'orch cancel',
        desc='cancels ongoing operations')
    def _cancel(self):
        """
        ProgressReferences might get stuck. Let's unstuck them.
        """
        self.cancel_completions()
        return HandleCommandResult()

    @_cli_read_command(
        'orch status',
        desc='Report configured backend and its status')
    def _status(self):
        o = self._select_orchestrator()
        if o is None:
            raise NoOrchestrator()

        avail, why = self.available()
        if avail is None:
            # The module does not report its availability
            return HandleCommandResult(stdout="Backend: {0}".format(o))
        else:
            return HandleCommandResult(stdout="Backend: {0}\nAvailable: {1}{2}".format(
                                           o, avail,
                                           " ({0})".format(why) if not avail else ""
                                       ))

    def self_test(self):
        old_orch = self._select_orchestrator()
        self._set_backend('')
        assert self._select_orchestrator() is None
        self._set_backend(old_orch)

        e1 = self.remote('selftest', 'remote_from_orchestrator_cli_self_test', "ZeroDivisionError")
        try:
            raise_if_exception(e1)
            assert False
        except ZeroDivisionError as e:
            assert e.args == ('hello', 'world')

        e2 = self.remote('selftest', 'remote_from_orchestrator_cli_self_test', "OrchestratorError")
        try:
            raise_if_exception(e2)
            assert False
        except OrchestratorError as e:
            assert e.args == ('hello', 'world')

        c = TrivialReadCompletion(result=True)
        assert c.has_result

    @_cli_write_command(
        'upgrade check',
        'name=image,type=CephString,req=false '
        'name=ceph_version,type=CephString,req=false',
        desc='Check service versions vs available and target containers')
    def _upgrade_check(self, image=None, ceph_version=None):
        completion = self.upgrade_check(image=image, version=ceph_version)
        self._orchestrator_wait([completion])
        raise_if_exception(completion)
        return HandleCommandResult(stdout=completion.result_str())

    @_cli_write_command(
        'upgrade status',
        desc='Check service versions vs available and target containers')
    def _upgrade_status(self):
        completion = self.upgrade_status()
        self._orchestrator_wait([completion])
        raise_if_exception(completion)
        r = {
            'target_image': completion.result.target_image,
            'in_progress': completion.result.in_progress,
            'services_complete': completion.result.services_complete,
            'message': completion.result.message,
        }
        out = json.dumps(r, indent=4)
        return HandleCommandResult(stdout=out)

    @_cli_write_command(
        'upgrade start',
        'name=image,type=CephString,req=false '
        'name=ceph_version,type=CephString,req=false',
        desc='Initiate upgrade')
    def _upgrade_start(self, image=None, ceph_version=None):
        completion = self.upgrade_start(image, ceph_version)
        self._orchestrator_wait([completion])
        raise_if_exception(completion)
        return HandleCommandResult(stdout=completion.result_str())

    @_cli_write_command(
        'upgrade pause',
        desc='Pause an in-progress upgrade')
    def _upgrade_pause(self):
        completion = self.upgrade_pause()
        self._orchestrator_wait([completion])
        raise_if_exception(completion)
        return HandleCommandResult(stdout=completion.result_str())

    @_cli_write_command(
        'upgrade resume',
        desc='Resume paused upgrade')
    def _upgrade_resume(self):
        completion = self.upgrade_resume()
        self._orchestrator_wait([completion])
        raise_if_exception(completion)
        return HandleCommandResult(stdout=completion.result_str())

    @_cli_write_command(
        'upgrade stop',
        desc='Stop an in-progress upgrade')
    def _upgrade_stop(self):
        completion = self.upgrade_stop()
        self._orchestrator_wait([completion])
        raise_if_exception(completion)
        return HandleCommandResult(stdout=completion.result_str())
