import argparse
import asyncio
import functools
import json
import os
import pprint
import psutil
import stat
import sys
import time
import traceback
import yaml
from copy import deepcopy

import logging
from colorlog import ColoredFormatter
log_fmt = logging.Formatter('%(lineno)-3d %(levelname)7s %(funcName)-26s %(message)s')
log_fmt = ColoredFormatter('%(log_color)s%(levelname)-8s %(lineno)4d %(funcName)-26s %(message)s',
    datefmt=None, reset=True,
    log_colors={'DEBUG':'cyan', 'INFO':'green', 'WARNING':'yellow', 'ERROR':'red', 'CRITICAL':'red'})
log_handler = logging.StreamHandler()
log_handler.setLevel(logging.DEBUG)
log_handler.setFormatter(log_fmt)
log = logging.getLogger(__file__)
log.addHandler(log_handler)

import logging.handlers
log_udp_handler = logging.handlers.DatagramHandler('127.0.0.1', 9000)
log_udp_handler.setLevel(logging.DEBUG)
log_udp_handler.setFormatter(log_fmt)
log.addHandler(log_udp_handler)

log.setLevel(logging.DEBUG)

class B4:
    conf = dict()
    daemon_dict = dict()
    def __init__(self):
        pass

b4 = B4()

def log_exc():
    if args.show_exc:
        log.warning('{}'.format(traceback.format_exc()))


async def daemon_do(reader, writer):
    while True:
        try:
            data = await reader.readuntil(b'\n\n')
            head_dict, body_len = parse_msg_head(data)

            cmd_list = head_dict['C'].split(' ')
            cmd = cmd_list[0]
            if cmd!='am':
                log.debug(head_dict)

            if cmd=='am':
                name, port = cmd_list[1:3]
                await daemon_do_am(reader, writer, name, port)
                continue
            elif cmd=='cp':
                src, dst = cmd_list[1:3]
                await daemon_do_cp(reader, writer, src, dst, body_len)
            elif cmd=='ln':
                src, dst = cmd_list[1:3]
                await daemon_do_ln(reader, writer, src, dst)
            elif cmd=='ls':
                dst = cmd_list[1] if len(cmd_list) > 1 else ''
                await daemon_do_ls(reader, writer, dst)
            elif cmd=='md':
                dst = cmd_list[1]
                await daemon_do_md(reader, writer, dst)
            elif cmd=='mv':
                src, dst = cmd_list[1:3]
                await daemon_do_mv(reader, writer, src, dst, body_len)
            elif cmd=='rm':
                dst = cmd_list[1]
                await daemon_do_rm(reader, writer, dst)

            writer.close()
            return

        except Exception:
            log_exc()
            writer.close()
            return


async def daemon_do_am(reader, writer, name, port):
    if 'tracker' in b4.conf:
        return

    peer_host, _, *_ = writer.get_extra_info('peername')
    b4.daemon_dict[name] = { 'host':peer_host, 'port':int(port), 'atime':time.time() }

    data = 'E:200\n\n'.encode('utf8')
    writer.write(data)


async def daemon_do_cp(reader, writer, src, dst, body_len):
    pass


async def daemon_do_ln(reader, writer, src, dst):
    dst_dir = os.path.dirname(dst)
    dir_abs = b4.conf['root'] + dst_dir
    log.debug('src {} dst {} dst_dir {} dir_abs {}'.format(src, dst, dst_dir, dir_abs))
    if not os.path.exists(dir_abs):
        writer.write('E:404 Not Found {}\n\n'.format(dir_abs).encode('utf8'))
        return
    if not os.path.isdir(dir_abs):
        writer.write('E:403 Forbidden {} is not folder\n\n'.format(dst_dir).encode('utf8'))
        return
    dst_abs = b4.conf['root'] + dst
    if not os.path.basename(dst):
        dst_abs = dst_abs + os.path.basename(src)
    if os.path.exists(dst_abs):
        writer.write('E:403 Forbidden {} is alreay exist\n\n'.format(dst).encode('utf8'))
        return
    with open(dst_abs, 'w') as f:
        f.write(src)
    writer.write('E:200 OK\n\n'.encode('utf8'))


async def daemon_do_ls(reader, writer, dst):
    log.debug('dst {}'.format(dst))
    if not dst:
        if 'tracker' in b4.conf:
            writer.close()
            return
        sock_host, _, *_ = writer.get_extra_info('sockname')
        b4.daemon_dict[b4.conf['name']] = {'host':sock_host, 'port':b4.conf['port'], 'atime':time.time() }
        data = '{}'.format(json.dumps(b4.daemon_dict)).encode('utf8')
        data = 'E:200\nL:{}\n\n'.format(len(data)).encode('utf8') + data
        writer.write(data)
        return
    if dst.startswith('/'):
        err, abs_path, show_link = parse_logic_path(dst)
        if err:
            writer.write(err)
            return
        err, data = daemon_do_ls_exec(abs_path, show_link)
        writer.write('E:{}\nL:{}\n\n'.format(err, len(data)).encode('utf8') + data)
        return
    await daemon_do_ls_host(reader, writer, dst)


def daemon_do_ls_exec(dst, show_link=False):
    ls_dict = {}
    host_name, host_path = dst.split('/', 1)
    if show_link:
        host_name = b4.conf['name']
        host_path = b4.conf['root'] + dst

    if not host_path:
        _list = psutil.disk_partitions()
        return '200 OK', '{}'.format(json.dumps(_list)).encode('utf8')

    log.debug('dst {} show_link {} host_path {}'.format(dst, show_link, host_path))

    if host_name!=b4.conf['name']:
        return '404 Not Found', '{}'.format(json.dumps(ls_dict)).encode('utf8')

    if not os.path.exists(host_path):
        return '404 Not Found', '{}'.format(json.dumps(ls_dict)).encode('utf8')

    if os.path.isfile(host_path):
        ls_dict[1] = {'type':'F', 'mtime':os.path.getmtime(host_path), 'size':os.path.getsize(host_path)}
        return '200 OK', '{}'.format(json.dumps(ls_dict)).encode('utf8')

    with os.scandir(host_path) as it:
        for entry in it:
            st = entry.stat()
            if stat.S_ISDIR(st.st_mode):
                ls_dict[entry.name] = {'type':'D', 'mtime':st.st_mtime}
            elif stat.S_ISREG(st.st_mode):
                if not show_link:
                    ls_dict[entry.name] = {'type':'F', 'mtime':st.st_mtime, 'size':st.st_size}
                else:
                    with open(host_path + '/' + entry.name) as f:
                        data = f.read()
                        ls_dict[entry.name] = {'type':data, 'mtime':st.st_mtime}
    return '200 OK', '{}'.format(json.dumps(ls_dict)).encode('utf8')


async def daemon_do_ls_host(reader, writer, dst):
    _list = dst.split('/', 1)
    name, path = _list if len(_list) > 1 else (dst, '')
    log.debug('name {} path {}'.format(name, path))
    dst = name + '/' + path
    if name!=b4.conf['name']:
        if name in b4.daemon_dict:
            writer.write('E:302 Found\nR:{} {} {}\n\n'.format(b4.daemon_dict[name]['host'], b4.daemon_dict[name]['port'], dst).encode('utf8'))
            return
        writer.write('E:503 Service Unavailable\n\n'.encode('utf8'))
        return
    err, data = await b4.loop.run_in_executor(None, functools.partial(daemon_do_ls_exec, dst))
    writer.write('E:{}\nL:{}\n\n'.format(err, len(data)).encode('utf8') + data)


async def daemon_do_md(reader, writer, dst):
    if not dst.startswith('/'):
        name, dst_abs = dst.split('/', 1)
        if name!=b4.conf['name']:
            writer.write('E:404 Not Found {}\n\n'.format(name).encode('utf8'))
            return
        try:
            os.mkdir(dst_abs)
        except:
            writer.write('E:403 Forbidden {}'.format(dst_abs).encode('utf8'))
            return
        writer.write('E:200 OK\n\n'.encode('utf8'))
        return

    dst_dir = os.path.dirname(dst)
    dir_abs = b4.conf['root'] + dst_dir
    log.debug('dst {} dst_dir {} dir_abs {}'.format(dst, dst_dir, dir_abs))

    err, abs_path, show_link = parse_logic_path(dst)
    if err:
        writer.write(err)
        return

    if not os.path.exists(dir_abs):
        writer.write('E:404 Not Found {}\n\n'.format(dir_abs).encode('utf8'))
        return

    
    if os.path.isfile(dir_abs):
        last = dst[len(dst_dir):]
        with open(dir_abs) as f:
            data = f.read()
            name, path = data.split('/', 1)
            if name!=b4.conf['name']:
                if name in b4.daemon_dict:
                    return 'E:302 Found\nR:{} {} {}\n\n'.format(b4.daemon_dict[name]['host'], b4.daemon_dict[name]['port'], data + last).encode('utf8'), None, None
                return 'E:503 Service Unavailable\n\n'.encode('utf8')
            os.mkdir(path + last)
            writer.write('E:200 OK\n\n'.encode('utf8'))
            return
    dst_abs = b4.conf['root'] + dst
    if os.path.exists(dst_abs):
        writer.write('E:403 Forbidden {} is alreay exist\n\n'.format(dst).encode('utf8'))
        return
    os.mkdir(dst_abs)
    writer.write('E:200 OK\n\n'.encode('utf8'))


async def daemon_do_mv(reader, writer, src, dst, body_len):
    pass


async def daemon_do_rm(reader, writer, dst):
    pass


async def daemon_task_send_am():
    reader, writer = None, None
    while True:
        await asyncio.sleep(1)
        try:
            if not writer:
                reader, writer = await asyncio.open_connection(b4.conf['tracker-host'], b4.conf['tracker-port'], loop=b4.loop)
            writer.write('C:am {} {}\n\n'.format(b4.conf['name'], b4.conf['port']).encode('utf8'))
            _data = await reader.readuntil(b'\n\n')
        except:
            log_exc()
            if writer:
                writer.close()
                writer = None


async def daemon_task_timeout_daemon():
    while True:
        await asyncio.sleep(1)
        daemon_dict = { k:v for k,v in b4.daemon_dict.items() }
        for name,v in daemon_dict.items():
            if name==b4.conf['name']:
                continue
            atime = v['atime']
            ts = time.time()
            if ts - atime > 5:
                log.debug('daemon timeout')
                b4.daemon_dict.pop(name, None)


def parse_link_file(path, last=''):
    with open(path) as f:
        data = f.read()
        name, phy_path = data.split('/', 1)
        if name!=b4.conf['name']:
            if name in b4.daemon_dict:
                return 'E:302 Found\nR:{} {} {}\n\n'.format(b4.daemon_dict[name]['host'], b4.daemon_dict[name]['port'], data + last).encode('utf8'), None
            return 'E:503 Service Unavailable\n\n'.encode('utf8'), None
        return None, phy_path + last


def parse_logic_path(dst_path):
    log.debug('path {}'.format(dst_path))
    cur_path = ''
    abs_path = b4.conf['root']
    p_list = dst_path.split('/')[1:]
    for p in p_list:
        pre_path = cur_path
        cur_path = cur_path + '/' + p
        abs_path = abs_path + '/' + p
        log.debug('pre {} cur {} abs {}'.format(pre_path, cur_path, abs_path))
        if os.path.exists(abs_path):
            if os.path.isdir(abs_path):
                if cur_path==dst_path:
                    return None, abs_path, True
            else:
                if cur_path==dst_path:
                    err, phy_path = parse_link_file(abs_path)
                    if err:
                        return err, None, None
                    return None, phy_path, False
        else:
            last = dst_path[len(pre_path):]
            pre_abs = b4.conf['root'] + pre_path
            log.debug('pre_abs {} last {}'.format(pre_abs, last))
            if os.path.isfile(pre_abs):
                err, phy_path = parse_link_file(pre_abs, last)
                if err:
                    return err, None, None
                return None, phy_path, False
            return 'E:404 Not Found\n\n'.encode('utf8'), None, None
    return 'E:404 Not Found\n\n'.encode('utf8'), None, None


def parse_msg_head(data):
    data = data.decode('utf8')
    head_list = data.split('\n')
    head_dict = dict()
    for head in head_list:
        word_list = head.split(':', 1)
        if len(word_list) < 2:
            continue
        k, v = word_list[:2]
        head_dict[k.strip()] = v.strip()
    body_len = 0
    if 'L' in head_dict:
        body_len = int(head_dict['L'])
    return head_dict, body_len


async def shell_do(*args):
    log.debug('{}'.format(args))
    if not args[0] in ['cp', 'ln', 'ls', 'md', 'mv', 'rm']:
        print('Unknown {}'.format(args[0]))
        return
    line = ' '.join(args)
    reader, writer = None, None
    try:
        reader, writer = await asyncio.open_connection(b4.conf['tracker-host'], b4.conf['tracker-port'], loop=b4.loop)
        writer.write('C:{}\n\n'.format(line).encode('utf8'))
        data = await reader.readuntil(b'\n\n')
        head_dict, body_len = parse_msg_head(data)
        print('{}'.format(head_dict))

        if 'R' in head_dict:
            host, port, path = head_dict['R'].split(' ', 2)[:3]
            writer.close()
            reader, writer = await asyncio.open_connection(host, int(port), loop=b4.loop)
            dst = '\nD:'+head_dict['D'] if 'D' in head_dict else ''
            writer.write('C:{} {}{}\n\n'.format(line.split(' ')[0], path, dst).encode('utf8'))
            data = await reader.readuntil(b'\n\n')
            head_dict, body_len = parse_msg_head(data)
            print('{}'.format(head_dict))

        if body_len:
            data = await reader.readexactly(body_len)
            data = data.decode('utf8')
            data = json.loads(data)
            print('{}'.format(pprint.pformat(data)))

    except:
        log_exc()
    if writer:
        writer.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='b4pns daemon and shell all-in-one')
    parser.add_argument('-c', dest='config', required=True, help='config file path')
    parser.add_argument('-e', dest='show_exc', default=False, action='store_true', help='show exception message')
    parser.add_argument('-m', dest='mode', required=True, help='mode: daemon or shell')
    parser.add_argument('rest', nargs=argparse.REMAINDER)

    args = parser.parse_args()

    config_path = os.path.dirname(os.path.abspath(__file__)) + '/' + os.path.splitext(os.path.basename(__file__))[0] + '.yml'
    if args.config:
        config_path = args.config

    with open(config_path) as f:
        b4.conf = yaml.load(f.read())
        log.debug('{}'.format(pprint.pformat(b4.conf)))

    import signal
    signal.signal(signal.SIGINT, signal.SIG_DFL)

    log.debug('{}'.format(sys.platform))
    if sys.platform == 'win32':
        asyncio.set_event_loop(asyncio.ProactorEventLoop())

    b4.loop = asyncio.get_event_loop()

    if 'tracker' in b4.conf:
        b4.conf['tracker-host'], b4.conf['tracker-port'] = b4.conf['tracker'].split(' ')

    if args.mode == 'shell':
        log.debug('shell {}'.format(args.rest))
        b4.loop.run_until_complete(shell_do(*args.rest))
    elif args.mode == 'daemon':
        coro = asyncio.start_server(daemon_do, None, b4.conf['port'], loop=b4.loop)
        server = b4.loop.run_until_complete(coro)
        log.debug('daemon listen {}'.format(b4.conf['port']))
        if 'tracker' in b4.conf:
            b4.loop.create_task(daemon_task_send_am())
        else:
            b4.daemon_dict[b4.conf['name']] = {'port':b4.conf['port']}
            b4.conf['root'] = b4.conf['root'].rstrip('/')
            if not os.path.isdir(b4.conf['root']):
                log.error('{} is not dir'.format(b4.conf['root']))
                exit()
            b4.loop.create_task(daemon_task_timeout_daemon())
        b4.loop.run_forever()
        server.close()
        b4.loop.run_until_complete(server.wait_closed())
    else:
        parser.print_help()

