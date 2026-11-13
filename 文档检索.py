#!/usr/bin/env python
# -*- coding: utf-8 -*-
r"""
DocFind CLI —— 面向大型 txt/log/csv/sql 的字段检索工具（流式逐行，不把文件读进内存）

常用示例：
  python 文档检索.py 12345  data/
  python 文档检索.py 12345  "D:/logs/*.txt" -C 2
  python 文档检索.py 12345  "D:/db_dump/*.sql" --mode word -C 2
  python 文档检索.py 12345  data/ --mode word          # 123456 不会被 12345 误命中
  python 文档检索.py 12345  data/ --mode field --sep , # 只匹配完整字段值
  python 文档检索.py "ORD-\\d{5}" data/ --regex -i
  python 文档检索.py -k 12345 -k 67890 --and data/     # 一行内同时含两个值
  python 文档检索.py 12345  data/ --export result.csv --jobs 4
  python 文档检索.py 12345  data/ --count              # 只统计数量
  python 文档检索.py --merge all.txt data/*.txt        # 把多个 txt 合并为一个
  python 文档检索.py --merge all.sql "D:/dump/*.sql" --merge-sep name --merge-encoding gb18030
  python 文档检索.py 12345  "D:/mysql/data/"          # 自动识别 .frm/.MYD/.MYI 二进制表文件
  python 文档检索.py --mysql-guide "D:/mysql/data/"   # 三件套关系说明 + 导出 SQL 的操作步骤
  python 文档检索.py 12345  table.MYD --binary force  # 强制按二进制扫描任意文件
  type big.txt | python 文档检索.py 12345 -            # 从标准输入读（管道）
  python 文档检索.py 12345 huge.log --checkpoint job.json   # 随时 Ctrl+C，之后可续查
  python 文档检索.py --resume job.json                      # 从上次断点接着查

随时停 / 看结果 / 继续查：
  * 按 Ctrl+C 不会丢结果：扫描会在当前行（二进制文件为当前数据块）收尾，
    照常打印已命中的行并导出，同时把断点写进 --checkpoint 指定的 JSON。
  * --resume 会恢复上次的全部检索参数与已得结果，跳过已扫完的文件，
    并从上次记录的行号 / 精确字节偏移继续，不重扫、不重复、不漏行（含上下文状态）。
  * 断点模式自动按单文件顺序扫描（忽略 -j）；UTF-16 文件按文件级断点（中断后整个文件重扫）。
  * 命中上限 --limit 触发的停止也会写断点：调高上限后 --resume 可接着查。

合并模式会逐文件自动识别编码，再按 --merge-encoding 统一写出（默认 UTF-8 带 BOM），
UTF-8 / GBK / UTF-16 混用也不会乱码；逐行流式写入，不把文件读进内存。

编码自动识别 UTF-8/UTF-8-BOM、UTF-16(BOM)、GBK/GB18030，识别不了可用 --encoding 指定。

MySQL MyISAM 三件套 .frm / .MYD / .MYI 是二进制格式，没有「行」：
  自动按字节流扫描，抽取可打印片段（字符串字段值、.frm 里的表名/字段名），
  并把纯数字关键词额外按 2/3/4/8 字节小端整数编码搜索（INT 型 12345 也能命中）。
  结果里的编号是十六进制字节偏移，不是行号。数值编码命中可能误报，可用 --no-bin-number 关闭。
  片段中文解码跟随 --encoding（auto 时在 UTF-8 / GB18030 间自动判断），表是 GBK 库就指定
  --encoding gb18030。日期/浮点/压缩表/InnoDB(.ibd) 无法这样可靠检索，建议先导出 SQL 再查。
  .MYI 只存索引键值 + 指向 .MYD 的指针，不含记录正文，不能只靠它查数据，也不会自动给 .MYD
  建索引；同名三件套会被关联显示，详细原因与 myisamchk / mysqldump 步骤见 --mysql-guide。
"""

import argparse
import csv
import glob
import json
import os
import re
import signal
import sys
import threading
import time
from collections import deque
from concurrent.futures import ThreadPoolExecutor, as_completed

DEFAULT_EXTS = ('.txt', '.log', '.csv', '.tsv', '.md', '.json', '.jsonl', '.ini', '.dat', '.sql',
                '.frm', '.myd', '.myi')
BIN_EXTS = ('.frm', '.myd', '.myi')       # MySQL MyISAM 二进制表文件
BIN_CHUNK = 1 << 20
BIN_OVERLAP = 4096
BIN_MIN_RUN = 4
COLOR_HIT = '\033[43;30m'
COLOR_DIM = '\033[90m'
COLOR_RESET = '\033[0m'


def setup_stdout():
    """统一按 UTF-8 处理，避免中文在 Windows 控制台 / 管道里乱码。"""
    try:
        sys.stdin.reconfigure(encoding='utf-8', errors='replace')
    except Exception:
        pass
    for stream in (sys.stdout, sys.stderr):
        try:
            if stream.encoding and stream.encoding.lower().replace('-', '') != 'utf8':
                stream.reconfigure(encoding='utf-8', errors='replace')
        except Exception:
            pass


def detect_encoding(path, sample=65536):
    with open(path, 'rb') as f:
        head = f.read(sample)
    if head.startswith(b'\xef\xbb\xbf'):
        return 'utf-8-sig'
    if head.startswith(b'\xff\xfe') or head.startswith(b'\xfe\xff'):
        return 'utf-16'
    if head and head.count(0) > len(head) * 0.25:
        return 'utf-16-le'
    for cut in range(4):
        chunk = head[:len(head) - cut] if cut else head
        try:
            chunk.decode('utf-8')
            return 'utf-8-sig'
        except UnicodeDecodeError:
            continue
    return 'gb18030'


def expand_paths(paths, exts):
    """把目录 / glob / 单文件展开成文件列表。显式单文件不受扩展名过滤限制。"""
    found = []
    for p in paths:
        if os.path.isdir(p):
            for root, _dirs, names in os.walk(p):
                for name in names:
                    if not exts or name.lower().endswith(exts):
                        found.append(os.path.join(root, name))
        elif any(c in p for c in '*?['):
            for m in glob.glob(p, recursive=True):
                if os.path.isfile(m) and (not exts or m.lower().endswith(exts)):
                    found.append(m)
        elif os.path.isfile(p):
            found.append(p)
        else:
            print('[警告] 路径不存在或不是文件：%s' % p, file=sys.stderr)
    return sorted(set(found))


class Matcher:
    """把检索条件编译成一个可反复调用的函数。"""

    def __init__(self, terms, mode, ignore_case, logic_and, sep, col):
        self.terms = terms
        self.mode = mode
        self.ignore_case = ignore_case
        self.logic_and = logic_and
        self.sep = '\t' if sep == '\\t' else sep
        self.col = col
        self._compile()

    def _compile(self):
        flags = re.IGNORECASE if self.ignore_case else 0
        if self.mode == 'regex':
            self.res = [re.compile(t, flags) for t in self.terms]
            self.hl_res = self.res
        else:
            if self.mode == 'word':
                # 只把 ASCII 字母数字下划线视为「词内字符」，中文紧邻数字时仍算边界
                self.res = [re.compile(r'(?<![0-9A-Za-z_])(%s)(?![0-9A-Za-z_])' % re.escape(t), flags)
                            for t in self.terms]
            else:
                self.res = [re.compile(re.escape(t), flags) for t in self.terms]
            # 高亮只覆盖关键词本身，不带整词边界的前导字符
            self.hl_res = [re.compile(re.escape(t), flags) for t in self.terms]

    def _cells(self, line):
        return [c.strip() for c in line.split(self.sep)]

    def test(self, line):
        if self.mode == 'field':
            cells = self._cells(line)
            if self.col is not None:
                cells = [cells[self.col - 1]] if 0 < self.col <= len(cells) else []
            norm = (lambda s: s.lower()) if self.ignore_case else (lambda s: s)
            terms = [norm(t) for t in self.terms]
            vals = [norm(c) for c in cells]
            checks = [t in vals for t in terms]
        else:
            checks = [bool(r.search(line)) for r in self.res]
        return all(checks) if self.logic_and else any(checks)

    def highlight(self, line, color):
        if not color:
            return line
        for r in self.hl_res:
            line = r.sub(lambda m: COLOR_HIT + m.group(0) + COLOR_RESET, line)
        return line


def iter_lines(path, encoding, from_stdin=False):
    if from_stdin:
        for i, line in enumerate(sys.stdin, 1):
            yield i, line.rstrip('\r\n')
        return
    with open(path, 'r', encoding=encoding, errors='replace', newline='') as f:
        for i, line in enumerate(f, 1):
            yield i, line.rstrip('\r\n')


def iter_lines_from(path, encoding, start, start_no):
    """断点续读：按二进制行迭代，产出 (行号, 文本, 该行结束时的字节偏移)。

    换行字节 0x0A 不会出现在 UTF-8 / GB18030 / Big5 / latin-1 的多字节字符内部，
    所以按字节切行是安全的，seek 到记录偏移就能精确续读，不重扫也不漏行。
    UTF-16 的换行是两个字节（0A 00），不适用，由调用方退化为文件级断点。
    """
    no = start_no
    with open(path, 'rb') as f:
        if start:
            f.seek(start)
        for raw in f:
            no += 1
            yield no, raw.decode(encoding, errors='replace').rstrip('\r\n'), f.tell()


# ---------- 断点续查 ----------
CHECKPOINT_VERSION = 1


def save_checkpoint(path, data):
    data = dict(data)
    data['version'] = CHECKPOINT_VERSION
    tmp = path + '.tmp'
    with open(tmp, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False)
    os.replace(tmp, path)


def load_checkpoint(path):
    with open(path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    if data.get('version') != CHECKPOINT_VERSION:
        raise ValueError('断点文件版本不匹配（version=%r）' % data.get('version'))
    if not data.get('argv'):
        raise ValueError('断点文件缺少原始命令参数，无法自动恢复')
    return data


def clean_argv(argv):
    """把 --resume 从原始命令里去掉，避免「续跑」动作本身被存进断点。"""
    out, skip = [], False
    for a in argv:
        if skip:
            skip = False
            continue
        if a == '--resume':
            skip = True
        elif not a.startswith('--resume='):
            out.append(a)
    return out


def clean_overrides(tokens, parser):
    """--resume 时只保留「选项 + 它的值」，丢掉误给的位置参数。

    这样 `docfind.py --resume job.json --limit 0` 能覆盖断点里的 --limit，
    而随手多写的关键词不会被当成额外的检索文件。
    """
    valued = set()
    for act in parser._actions:
        if act.nargs != 0:
            valued.update(act.option_strings)
    out, expect = [], False
    for a in tokens:
        if expect:
            out.append(a)
            expect = False
        elif a.startswith('-'):
            out.append(a)
            expect = ('=' not in a) and (a.split('=', 1)[0] in valued)
    return out


def apply_overrides(base, overrides, parser):
    """把续跑时附加的覆盖参数合并进原命令：同名选项只留最后一次，避免命令行无限累积。"""
    valued = set()
    for act in parser._actions:
        if act.nargs != 0:
            valued.update(act.option_strings)

    def spans(tokens):
        out, i = [], 0
        while i < len(tokens):
            a = tokens[i]
            if a.startswith('-'):
                key = a.split('=', 1)[0]
                end = i + 1
                if ('=' not in a and key in valued and end < len(tokens)
                        and not tokens[end].startswith('-')):
                    end += 1
                out.append((key, i, end))
                i = end
            else:
                i += 1
        return out

    ov_keys = {k for k, _s, _e in spans(overrides)} - {'-k', '--keyword'}
    out, i = [], 0
    for key, s, e in spans(base):
        out.extend(base[i:s])            # 两个选项之间的位置参数原样保留
        if key not in ov_keys:
            out.extend(base[s:e])
        i = e
    out.extend(base[i:])
    return out + overrides


# ---------- MySQL MyISAM / 二进制文件 ----------
MYSQL_GUIDE = r"""
【为什么不能只靠 .MYI】
  .MYI 是 B-tree 索引文件，只保存「索引列的键值 + 指向 .MYD 记录的指针」，
  不保存其它字段的正文；键值还经过前缀压缩 / 二进制编码，未必有明文可搜。
  没有 .MYD 就没有记录内容，没有 .frm 就不知道每列的类型与长度，无法把记录切成字段。
  工具里的「建索引」只是把扫描结果缓存起来、换关键词不重扫，与 .MYI 完全无关。

【让 .MYI 真正索引 .MYD = 交给 MySQL 引擎】
  1) 把同名三件套放进同一个库目录（MySQL/MariaDB 数据目录），版本尽量一致，
     .frm 结构不兼容是最常见的坑；MySQL 8.0 建议改用 MariaDB 或同版本实例。
  2) myisamchk -dvv <表>.MYI          查看表 / 索引元信息，不必启动服务
  3) 启动服务后导出成文本：
     mysqldump -u root -p <db> <表> > <表>.sql
  4) 再用本工具检索导出的 .sql（文本模式，整词 / 字段相等 / 正则都可用）

【只想找 12345 在不在、大致在哪】
  只传 .MYD 就够：本工具按字节流扫描，能找明文字符串和 2/3/4/8B 小端整数编码。
  离线通用解析 MyISAM 记录布局（fixed / dynamic / compressed + 键压缩）跨版本极易出错，
  本工具刻意不实现，避免给出看似正确、实则字段错位的结果。
"""


def mysql_sets(paths):
    """把同名 .frm/.MYD/.MYI 关联成「表」。返回 [{name, dir, parts}]。"""
    sets = {}
    for p in paths:
        if p == '-':
            continue
        d, base = os.path.split(os.path.normpath(p))
        stem, ext = os.path.splitext(base)
        ext = ext.lower()
        if ext not in BIN_EXTS:
            continue
        key = os.path.join(d, stem)
        sets.setdefault(key, {'name': stem, 'dir': d, 'parts': {}})
        sets[key]['parts'][ext] = p
    return list(sets.values())


def mysql_guide_text(sets):
    out = ['=' * 60, 'MyISAM 三件套（.frm / .MYD / .MYI）关系与操作指引', '=' * 60]
    if sets:
        for s in sets:
            tags = '  '.join('%s %s' % (e.upper(), '有' if e in s['parts'] else '缺')
                             for e in BIN_EXTS)
            out.append('表 %-24s %s' % (s['name'], tags))
            if s['dir']:
                out.append('  目录: %s' % s['dir'])
            if '.myi' in s['parts'] and '.myd' not in s['parts']:
                out.append('  [注意] 只有索引文件，没有数据文件，查不到记录正文')
        out.append('')
    out.append(MYSQL_GUIDE.strip())
    return '\n'.join(out)


def is_binary_path(path, mode):
    if mode == 'force':
        return True
    if mode == 'off':
        return False
    return path.lower().endswith(BIN_EXTS)


def _bin_ok(b):
    return 0x20 <= b <= 0x7E or b == 0x09 or b >= 0x80


def _bin_text(bs, enc='auto'):
    """把一段字节尽量解成可读文本；控制字符统一显示成点。

    --encoding 指定了具体字符集时按它解（表是 GBK 库时中文才不会乱）。
    auto 时先按 UTF-8 容错解，替换符太多才回退 GB18030。
    """
    if enc and enc != 'auto':
        try:
            s = bs.decode(enc, errors='replace')
        except LookupError:
            s = bs.decode('utf-8', errors='replace')
    else:
        s = bs.decode('utf-8', errors='replace')
        if s.count('\ufffd') > max(1, len(s) * 0.2):
            s = bs.decode('gb18030', errors='replace')
    return ''.join(c if c == '\t' or (ord(c) >= 32 and ord(c) != 127 and c != '\ufffd') else '.'
                   for c in s)


def _bin_runs(buf, base, enc='auto'):
    """产出 (起始绝对偏移, 可打印片段文本, 结束绝对偏移)。

    结束偏移用于把断点精确落在「最后一条已输出命中」之后；连续可打印字节
    不少于 BIN_MIN_RUN 才算一个片段。
    """
    i, n = 0, len(buf)
    while i < n:
        if not _bin_ok(buf[i]):
            i += 1
            continue
        j = i
        while j < n and _bin_ok(buf[j]):
            j += 1
        if j - i >= BIN_MIN_RUN:
            yield base + i, _bin_text(buf[i:j], enc), base + j
        i = j


def numeric_needles(terms):
    """纯数字关键词 -> MyISAM 定长整数的小端编码候选 (字节串, 原词, 宽度)。"""
    out, seen = [], set()
    for t in terms:
        if not re.fullmatch(r'\d{1,20}', t):
            continue
        v = int(t)
        for width in (2, 3, 4, 8):
            if v >= (1 << (width * 8)):
                continue
            raw = v.to_bytes(width, 'little')
            if raw not in seen:
                seen.add(raw)
                out.append((raw, t, width))
    return out


def scan_binary_file(path, matcher, args, stop_event, progress, resume=None, state=None):
    """按字节流扫描二进制文件，返回 (命中列表, 片段数, 字节数, 'binary')。

    命中编号是十六进制字节偏移字符串（如 0x1A2B），不是行号。
    resume/state 非空时按「已消费字节偏移 + 未处理的 carry」续读。
    """
    hits, runs, size = [], 0, 0
    needles = [] if args.no_bin_number else numeric_needles(args.terms)
    bin_enc = args.encoding          # 指定表字符集时，片段按它解码
    carry, base = b'', 0
    if resume:
        base = int(resume.get('offset', 0))
        carry = bytes.fromhex(resume.get('carry', '') or '')
        # carry 就是 base 之后、尚未处理的那一段；它已经被「读」过了，
        # 所以续读要从 base + len(carry) 开始，否则这段会被重复拼一次、后续数据整体错位。
        size = base + len(carry)
        runs = int(resume.get('runs', 0))
    total_size = os.path.getsize(path)
    if state is not None:
        state.update(offset=base, carry=carry.hex(), size=size, runs=runs,
                     no=runs, resumable=True)
    with open(path, 'rb') as f:
        seek_to = base + len(carry)
        if seek_to:
            f.seek(seek_to)
        while not stop_event.is_set():
            chunk = f.read(BIN_CHUNK)
            if not chunk:
                break
            size += len(chunk)
            buf = carry + chunk
            more = f.tell() < total_size
            if more:
                # 还有后续数据：切点回退到可打印片段边界，避免把一段文字/一条记录切成两半
                cut = max(0, len(buf) - BIN_OVERLAP)
                while cut > 0 and _bin_ok(buf[cut]):
                    cut -= 1
                if cut == 0:
                    cut = max(0, len(buf) - BIN_OVERLAP)
            else:
                cut = len(buf)
            view = buf[:cut]
            # 先收集本块全部命中，按绝对偏移排序后统一输出。这样 --limit / Ctrl+C
            # 停止时能把断点精确落在「最后一条已输出命中」之后，剩余数据留在 carry，
            # 续跑不重不漏（原先在块内 break、却把断点记在块末，会整块漏掉命中）。
            events = []                  # (起始绝对偏移, 结束绝对偏移, 文本)
            for off, text, end in _bin_runs(view, base, bin_enc):
                runs += 1
                progress(False)          # 命中数在真正输出时才累加
                if matcher.test(text):
                    events.append((off, end, text))

            if needles:
                # 2B/3B/4B/8B 是前缀关系（12345 -> 39 30 / 39 30 00 / ...），
                # 同一位置会连中多次；按 (偏移, 关键词) 聚合，只输出一条并列出所有可能宽度。
                found = {}
                for raw, term, width in needles:
                    pos, w = 0, len(raw)
                    while True:
                        # 在完整 buf 上找，允许 needle 跨过 cut 边界；只收录起点 < cut 的，
                        # 起点 >= cut 的留给下一轮 carry，避免重复统计
                        pos = buf.find(raw, pos)
                        if pos < 0 or pos >= cut:
                            break
                        key = (base + pos, term)
                        if key not in found:
                            found[key] = {'widths': set(), 'pos': pos}
                        found[key]['widths'].add(width)
                        pos += w
                for (abs_off, term), info in sorted(found.items()):
                    p = info['pos']
                    lo, hi = max(0, p - 20), min(len(buf), p + 8 + 20)
                    around = buf[lo:hi]
                    hexs = ' '.join('%02X' % b for b in around)
                    widths = '/'.join('%dB' % w for w in sorted(info['widths']))
                    label = ('[数值 %s · %s 小端 @0x%X] %s | %s'
                             % (term, widths, abs_off, hexs, _bin_text(around, bin_enc)))
                    events.append((abs_off, min(abs_off + 8, base + cut), label))
                    progress(False)

            events.sort(key=lambda e: e[0])
            consumed_end = base + cut
            for s, e, text in events:
                hits.append((path, '0x%X' % s, text, 'hit'))
                progress.add_hit()
                if args.limit and progress.total_hits >= args.limit:
                    stop_event.set()
                    consumed_end = e
                    break

            if stop_event.is_set() and consumed_end < base + cut:
                # 块内提前停止：断点之后的数据还没被处理，续跑时从这个偏移重新读即可，
                # 不必把大半个数据块塞进 checkpoint（转 hex 后会膨胀一倍）。
                carry = b''
                base = consumed_end
                size = consumed_end
            else:
                carry = buf[cut:]
                base += cut
            if state is not None:
                state.update(offset=base, carry=carry.hex(), size=size, runs=runs,
                             no=runs, resumable=True)
    return hits, runs, size, 'binary'


def scan_file(path, matcher, args, stop_event, progress, resume=None, state=None):
    """扫描单个文件，返回 (命中列表, 扫描行数, 字节数, 编码)。

    resume/state 非空时启用精确字节断点：从 resume['offset'] 续读，退出时把
    (行号, 字节偏移, 上下文 ring/tail) 写进 state 供断点文件保存。
    """
    hits = []
    if args.stdin:
        encoding = 'utf-8'          # 管道/标准输入没有实体文件，不做编码探测
    else:
        encoding = args.encoding if args.encoding != 'auto' else detect_encoding(path)
    size = 0 if args.stdin else os.path.getsize(path)
    ctx = args.context
    lines, last_off = 0, 0
    if resume:
        lines = int(resume.get('no', 0))
        last_off = int(resume.get('offset', 0))
        ring = deque(resume.get('ring', []), maxlen=max(ctx, 1))
        tail = int(resume.get('tail', 0))
        last_printed = int(resume.get('last_printed', -1))
    else:
        ring = deque(maxlen=max(ctx, 1))
        tail = 0
        last_printed = -1

    # UTF-16 换行占两字节，按字节切行会切坏字符；这类文件退化为「整文件重扫」
    resumable = (state is not None and not args.stdin
                 and '16' not in encoding.lower().replace('_', '-'))
    if state is not None:
        state.update(no=lines, offset=last_off, ring=[list(r) for r in ring],
                     tail=tail, last_printed=last_printed,
                     resumable=resumable, size=size)

    def handle(no, text):
        nonlocal ring, tail, last_printed, lines
        hit = matcher.test(text)
        if hit:
            for cno, ctext in ring:
                if cno > last_printed:
                    hits.append((path, cno, ctext, 'ctx'))
                    last_printed = cno
            ring.clear()
            if no > last_printed:
                hits.append((path, no, text, 'hit'))
                last_printed = no
            tail = ctx
        elif tail > 0:
            if no > last_printed:
                hits.append((path, no, text, 'ctx'))
                last_printed = no
            tail -= 1
        elif ctx:
            ring.append((no, text))
        lines = no
        progress(hit)
        if args.limit and progress.total_hits >= args.limit:
            stop_event.set()
            return False
        return True

    if resumable:
        for no, text, off in iter_lines_from(path, encoding, last_off, lines):
            if stop_event.is_set():
                break
            ok = handle(no, text)       # 这一行已经处理过，才能把偏移推进到 off
            last_off = off
            if not ok:
                break
    else:
        for no, text in iter_lines(path, encoding, args.stdin):
            if stop_event.is_set():
                break
            if not handle(no, text):
                break

    if state is not None:
        state.update(no=lines, offset=last_off if resumable else 0,
                     ring=[list(r) for r in ring], tail=tail,
                     last_printed=last_printed, resumable=resumable, size=size)
    return hits, lines, size, encoding


class Progress:
    def __init__(self, args):
        self.args = args
        self.lock = threading.Lock()
        self.total_lines = 0
        self.total_hits = 0
        self.start = time.time()
        self.next_report = args.progress_every

    def add_hit(self):
        """只累加命中数（用于先收集、后按偏移统一输出的二进制模式）。"""
        with self.lock:
            self.total_hits += 1

    def __call__(self, hit):
        with self.lock:
            self.total_lines += 1
            if hit:
                self.total_hits += 1
            if (not self.args.quiet and not self.args.stdout_input
                    and self.total_lines >= self.next_report):
                self.next_report += self.args.progress_every
                elapsed = time.time() - self.start
                print('[进度] 已扫描 %s 行 / 命中 %s 处 / %.1fs'
                      % (format(self.total_lines, ','), format(self.total_hits, ','), elapsed),
                      file=sys.stderr, flush=True)


def print_hits(hits, matcher, args):
    color = args.color and sys.stdout.isatty()
    shown = 0
    prev_no = -9999
    prev_path = None
    for path, no, text, kind in hits:
        if args.count:
            continue
        if not args.context and kind == 'ctx':
            continue
        # 文本模式 no 是行号(int)，二进制模式是十六进制字节偏移(str)
        no_key = no if isinstance(no, int) else int(str(no), 16)
        if shown and (path != prev_path or prev_no < no_key - 1):
            print(COLOR_DIM + '  ---' + COLOR_RESET if color else '  ---')
        prefix = '>>' if kind == 'hit' else '  '
        body = matcher.highlight(text, color) if kind == 'hit' else text
        if color and kind == 'ctx':
            body = COLOR_DIM + body + COLOR_RESET
        print('%s %s:%s: %s' % (prefix, path, no, body))
        prev_path = path
        prev_no = no_key
        shown += 1
        if args.max_print and shown >= args.max_print:
            print('... 屏幕输出已达 %d 行上限，完整结果请用 --export 导出' % args.max_print)
            break


def export_hits(hits, args):
    if not args.export:
        return
    out = args.export
    fmt = args.format
    if fmt == 'auto':
        fmt = 'json' if out.lower().endswith('.json') else ('txt' if out.lower().endswith('.txt') else 'csv')
    rows = [h for h in hits if h[3] == 'hit']
    if fmt == 'csv':
        with open(out, 'w', encoding='utf-8-sig', newline='') as f:
            w = csv.writer(f)
            w.writerow(['文件', '行号', '内容'])
            for path, no, text, _k in rows:
                w.writerow([path, no, text])
    elif fmt == 'json':
        with open(out, 'w', encoding='utf-8') as f:
            json.dump({'query': args.terms, 'count': len(rows),
                       'results': [{'file': p, 'line': n, 'text': t} for p, n, t, _k in rows]},
                      f, ensure_ascii=False, indent=2)
    else:
        with open(out, 'w', encoding='utf-8') as f:
            f.write('# DocFind 检索结果  query=%s\n' % ' | '.join(args.terms))
            for path, no, text, _k in rows:
                f.write('%s:%s: %s\n' % (path, no, text))
    print('[导出] %d 条命中 -> %s' % (len(rows), os.path.abspath(out)))


def merge_files(paths, out, out_enc, sep_mode):
    """按给定顺序把多个文本流式合并为一个；逐文件识别编码，统一编码写出。"""
    t0 = time.time()
    out_abs = os.path.abspath(out)
    paths = [p for p in paths if os.path.abspath(p) != out_abs]  # 别把输出文件自己并进去
    # .frm/.MYD/.MYI 是二进制，按文本解码合并只会得到乱码，直接跳过
    kept = [p for p in paths if not p.lower().endswith(BIN_EXTS)]
    if len(kept) < len(paths):
        print('[提示] 已跳过 %d 个 MySQL 二进制表文件（.frm/.MYD/.MYI），合并仅处理文本文件'
              % (len(paths) - len(kept)), file=sys.stderr)
    paths = kept
    if not paths:
        raise ValueError('没有可合并的文本文件（二进制表文件不参与合并）')
    total_in = 0
    with open(out, 'w', encoding=out_enc, errors='replace', newline='') as w:
        for i, p in enumerate(paths):
            enc = detect_encoding(p)
            if sep_mode == 'name':
                w.write(('===== %s =====\r\n' if i == 0 else '\r\n===== %s =====\r\n') % p)
            elif i > 0 and sep_mode == 'newline':
                w.write('\r\n')
            last = ''
            with open(p, 'r', encoding=enc, errors='replace', newline='') as f:
                for line in f:
                    w.write(line)
                    if line:
                        last = line[-1]
            if last and last not in ('\n', '\r'):   # 文件末尾没有换行时补一个，避免两文件粘行
                w.write('\n')
            total_in += os.path.getsize(p)
    return len(paths), total_in, os.path.getsize(out), time.time() - t0


def build_parser():
    p = argparse.ArgumentParser(
        description='大文本字段检索：输入 12345 定位所在行；--merge 可把多个 txt 合并为一个',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__)
    p.add_argument('pattern', nargs='?', help='要检索的字段值 / 正则表达式')
    p.add_argument('paths', nargs='*', help='文件、目录或通配符；用 - 表示从标准输入读取')
    p.add_argument('-k', '--keyword', action='append', default=[], help='追加关键词，可多次使用')
    p.add_argument('-m', '--mode', choices=['contains', 'word', 'field', 'regex'], default='contains',
                   help='contains=子串(默认) word=完整编号 field=字段相等 regex=正则')
    p.add_argument('--regex', action='store_true', help='正则模式，等同于 --mode regex')
    p.add_argument('-i', '--ignore-case', action='store_true', help='忽略大小写')
    p.add_argument('--and', dest='logic_and', action='store_true', help='多关键词需全部命中（默认任一命中）')
    p.add_argument('--sep', default=',', help='field 模式的列分隔符，默认逗号；\\t 表示制表符')
    p.add_argument('--col', type=int, default=None, help='field 模式只匹配第 N 列（从 1 开始）')
    p.add_argument('-C', '--context', type=int, default=0, help='连带输出命中行前后 N 行，默认 0')
    p.add_argument('-n', '--limit', type=int, default=2000, help='最多命中条数，0=不限，默认 2000')
    p.add_argument('--max-print', type=int, default=500, help='屏幕最多输出行数，0=不限，默认 500')
    p.add_argument('--encoding', default='auto',
                   help='auto(默认)/utf-8-sig/gb18030/utf-16/gbk/big5/latin-1；'
                        '二进制模式下同时决定片段中文的解码字符集')
    p.add_argument('--ext', default=','.join(DEFAULT_EXTS), help='目录递归时的扩展名过滤，逗号分隔；空串=全部')
    p.add_argument('--binary', choices=['auto', 'force', 'off'], default='auto',
                   help='二进制扫描：auto=只处理 .frm/.MYD/.MYI(默认) force=全部按字节流 off=全部当文本')
    p.add_argument('--no-bin-number', action='store_true',
                   help='二进制扫描时不搜索数字的定长整数编码（可减少误报）')
    p.add_argument('--mysql-guide', action='store_true',
                   help='只打印 MyISAM 三件套的关系说明与「导出 SQL 再检索」的操作步骤，不检索')
    p.add_argument('-e', '--export', help='导出结果到文件（.csv/.txt/.json）')
    p.add_argument('--format', choices=['auto', 'csv', 'txt', 'json'], default='auto')
    p.add_argument('-j', '--jobs', type=int, default=1, help='并行扫描的文件数，默认 1')
    p.add_argument('--count', action='store_true', help='只统计命中数，不输出内容')
    p.add_argument('--no-color', action='store_true', help='关闭颜色高亮')
    p.add_argument('-q', '--quiet', action='store_true', help='不显示进度')
    p.add_argument('--progress-every', type=int, default=500000, help='每扫描多少行打印一次进度')
    p.add_argument('--checkpoint', metavar='FILE',
                   help='把进度写入 JSON 断点；随时 Ctrl+C 收尾，之后用 --resume 从断点继续')
    p.add_argument('--resume', metavar='FILE',
                   help='从断点继续：自动恢复上次的关键词、参数、文件列表与已得结果')
    p.add_argument('--merge', metavar='OUTPUT',
                   help='合并模式：把位置参数里的文件按序合并写入 OUTPUT（此时不需要检索词）')
    p.add_argument('--merge-sep', choices=['newline', 'name', 'none'], default='newline',
                   help='合并时文件之间的分隔：newline=空一行(默认) name=插入文件名标记 none=直接拼接')
    p.add_argument('--merge-encoding', choices=['utf-8-sig', 'utf-8', 'gb18030', 'utf-16'],
                   default='utf-8-sig', help='合并输出编码，默认 UTF-8 带 BOM（Windows 记事本友好）')
    return p


def main(argv=None):
    setup_stdout()
    original_argv = list(sys.argv[1:] if argv is None else argv)
    raw_argv = list(original_argv)

    # --resume 会在正式解析前把上次的完整命令行换回来，所以用户不需要再输一遍关键词/参数
    resume_file, cp = None, {}
    for i, a in enumerate(raw_argv):
        if a == '--resume' and i + 1 < len(raw_argv):
            resume_file = raw_argv[i + 1]
        elif a.startswith('--resume='):
            resume_file = a.split('=', 1)[1]
    if resume_file:
        try:
            cp = load_checkpoint(resume_file)
            parser_for_merge = build_parser()
            overrides = clean_overrides(clean_argv(original_argv), parser_for_merge)
            raw_argv = apply_overrides(list(cp['argv']), overrides, parser_for_merge)
            cp['argv'] = list(raw_argv)     # 之后的断点沿用这次实际生效的参数
        except (OSError, ValueError) as e:
            print('[错误] 读取断点失败：%s' % e, file=sys.stderr)
            return 2
        print('[断点] 已恢复上次检索：%s' % ' '.join(raw_argv))
        if overrides:
            print('[断点] 本次附加覆盖：%s' % ' '.join(overrides))

    args = build_parser().parse_args(raw_argv)
    args.color = not args.no_color
    if args.regex:
        args.mode = 'regex'
    args.stdin = '-' in args.paths
    args.stdout_input = args.stdin
    exts = tuple('.' + e.strip().lstrip('.').lower() for e in args.ext.split(',') if e.strip()) if args.ext else ()

    # 单独的「三件套说明」模式：不检索，只打印关系与导出步骤
    if args.mysql_guide:
        raw = ([args.pattern] if args.pattern else []) + [p for p in args.paths if p != '-']
        found = expand_paths(raw, exts) if raw else []
        print(mysql_guide_text(mysql_sets(found)))
        return 0

    if not args.merge and not args.paths:
        print('[错误] 请提供要检索的文件、目录或通配符（用 - 表示从标准输入读）', file=sys.stderr)
        return 2

    # 合并模式：argparse 会把第一个位置参数分给 pattern，所以两者都当作输入文件
    if args.merge:
        if args.stdin:
            print('[错误] 合并模式不支持标准输入 -', file=sys.stderr)
            return 2
        merge_paths = ([args.pattern] if args.pattern else []) + list(args.paths)
        targets = expand_paths(merge_paths, exts)
        try:
            n, size_in, size_out, sec = merge_files(targets, args.merge, args.merge_encoding, args.merge_sep)
        except (OSError, ValueError) as e:
            print('[错误] 合并失败：%s' % e, file=sys.stderr)
            return 2
        print('[合并] %d 个文件 -> %s' % (n, os.path.abspath(args.merge)))
        print('       读入 %.2f MB / 写出 %.2f MB / %.2fs / 编码 %s / 分隔 %s'
              % (size_in / 1048576, size_out / 1048576, sec, args.merge_encoding, args.merge_sep))
        return 0

    terms = ([args.pattern] if args.pattern else []) + list(args.keyword)
    if not terms:
        print('[错误] 请提供检索内容（位置参数或 -k）', file=sys.stderr)
        return 2
    args.terms = terms
    if args.mode == 'regex':
        try:
            [re.compile(t, re.IGNORECASE if args.ignore_case else 0) for t in terms]
        except re.error as e:
            print('[错误] 正则表达式有误：%s' % e, file=sys.stderr)
            return 2

    paths = ['-'] if args.stdin else expand_paths(args.paths, exts)
    if not paths:
        print('[错误] 没有找到可检索的文件', file=sys.stderr)
        return 2

    if cp.get('paths'):
        paths = list(cp['paths'])
    ckpt_path = args.checkpoint or args.resume
    ckpt_mode = bool(ckpt_path)
    if ckpt_mode and (args.stdin or '-' in paths):
        print('[错误] 断点模式不支持标准输入 -', file=sys.stderr)
        return 2

    matcher = Matcher(terms, args.mode, args.ignore_case, args.logic_and, args.sep, args.col)
    progress = Progress(args)
    stop_event = threading.Event()
    all_hits, all_lines, all_bytes, enc_used = [], 0, 0, set()
    t0 = time.time()

    prev_hits = [tuple(h) for h in cp.get('hits', [])]
    done_files = set(cp.get('done', []))
    cur = cp.get('current')
    if prev_hits:
        all_hits.extend(prev_hits)
        progress.total_hits = sum(1 for h in prev_hits if h[3] == 'hit')

    def work(path, resume=None, state=None):
        if path != '-' and is_binary_path(path, args.binary):
            return scan_binary_file(path, matcher, args, stop_event, progress, resume, state)
        return scan_file(path, matcher, args, stop_event, progress, resume, state)

    # Ctrl+C 只置停止标志，让扫描在当前行 / 当前数据块收尾，已命中的结果照常输出
    interrupted = False
    old_sigint = signal.getsignal(signal.SIGINT)

    def on_sigint(signum, frame):
        nonlocal interrupted
        if stop_event.is_set():          # 再按一次就恢复系统默认行为，强制退出
            signal.signal(signal.SIGINT, signal.SIG_DFL)
            raise KeyboardInterrupt
        interrupted = True
        stop_event.set()
        print('\n[中断] 已收到 Ctrl+C，正在收尾…已命中的结果会照常输出（再按一次强制退出）',
              file=sys.stderr, flush=True)

    signal.signal(signal.SIGINT, on_sigint)

    def save_cp(current, rows=None):
        if not ckpt_path:
            return
        cp['argv'] = list(cp.get('argv') or clean_argv(raw_argv))
        cp['paths'] = list(paths)
        cp['done'] = sorted(done_files)
        cp['hits'] = [list(x) for x in (all_hits if rows is None else rows)]
        cp['current'] = current
        cp['stats'] = {'lines': all_lines, 'bytes': all_bytes}
        try:
            save_checkpoint(ckpt_path, cp)
        except OSError as e:
            print('[警告] 断点写入失败：%s' % e, file=sys.stderr)

    try:
        if ckpt_mode:
            # 断点模式强制顺序扫描，逐文件保存精确的 (行号, 字节偏移, 上下文状态)
            for path in [p for p in paths if p not in done_files]:
                if stop_event.is_set():
                    break
                state = {}
                r = cur if (cur and cur.get('path') == path) else None
                cur = None               # 断点只对第一个待续文件生效
                try:
                    h, l, b, enc = work(path, r, state)
                except (OSError, UnicodeError) as e:
                    print('[警告] 跳过文件：%s' % e, file=sys.stderr)
                    continue
                if stop_event.is_set() and state.get('resumable') is False:
                    # UTF-16 记不了行内偏移：丢掉本文件这次的部分命中，下次整文件重扫
                    print('[断点] %s 是 UTF-16，按文件级保存（下次会重扫这一个文件）'
                          % path, file=sys.stderr)
                    save_cp(None)
                    break
                all_hits.extend(h); all_lines += l; all_bytes += b; enc_used.add(enc)
                if stop_event.is_set() and state:
                    save_cp(dict(state, path=path,
                                 binary=is_binary_path(path, args.binary)))
                    break
                done_files.add(path)
                save_cp(None)
            else:
                if ckpt_path and os.path.exists(ckpt_path):
                    os.remove(ckpt_path)
                    print('[断点] 全部扫描完成，已清理断点文件 %s' % ckpt_path)
        elif args.stdin or args.jobs <= 1 or len(paths) == 1:
            for path in paths:
                if stop_event.is_set():
                    break
                try:
                    h, l, b, enc = work(path)
                except (OSError, UnicodeError) as e:
                    print('[警告] 跳过文件：%s' % e, file=sys.stderr)
                    continue
                all_hits.extend(h); all_lines += l; all_bytes += b; enc_used.add(enc)
        else:
            pool = ThreadPoolExecutor(max_workers=min(args.jobs, len(paths)))
            futures = [pool.submit(work, p) for p in paths]
            collected = set()
            try:
                for fu in as_completed(futures):
                    try:
                        h, l, b, enc = fu.result()
                    except (OSError, UnicodeError) as e:
                        print('[警告] 跳过文件：%s' % e, file=sys.stderr)
                        continue
                    all_hits.extend(h); all_lines += l; all_bytes += b; enc_used.add(enc)
                    collected.add(id(fu))
                    if stop_event.is_set():
                        break
            except KeyboardInterrupt:
                interrupted = True
                stop_event.set()
            finally:
                pool.shutdown(wait=True)   # stop_event 已置位，worker 会很快收尾
                for fu in futures:
                    if id(fu) in collected or fu.cancelled() or fu.exception():
                        continue
                    h, l, b, enc = fu.result()
                    all_hits.extend(h); all_lines += l; all_bytes += b; enc_used.add(enc)
    except KeyboardInterrupt:
        interrupted = True
        stop_event.set()
    finally:
        signal.signal(signal.SIGINT, old_sigint)

    if interrupted or stop_event.is_set():
        tail = ('；断点已保存，用 --resume %s 可接着查' % ckpt_path) if ckpt_path else \
               '；下次加 --checkpoint job.json 就能保存断点、从断点续查'
        print('[状态] 扫描提前停止，下面是到目前为止找到的结果' + tail, file=sys.stderr)

    all_hits.sort(key=lambda x: (x[0], x[1] if isinstance(x[1], int) else int(str(x[1]), 16)))
    hit_count = sum(1 for h in all_hits if h[3] == 'hit')
    elapsed = time.time() - t0
    bin_used = any(e == 'binary' for e in enc_used)
    sets = mysql_sets([p for p in paths if p != '-']) if bin_used else []
    if sets and not args.quiet:
        for s in sets:
            tags = ' '.join('%s%s' % (e.upper(), '' if e in s['parts'] else '(缺)')
                            for e in BIN_EXTS)
            print('[MyISAM] %-22s %s' % (s['name'], tags))
            if '.myi' in s['parts'] and '.myd' not in s['parts']:
                print('         [注意] 只有 .MYI 索引、没有 .MYD 数据：索引文件不含记录正文，'
                      '查不到其它字段')
        print('[MyISAM] .MYI 不会自动索引 .MYD；完整行查询需导出 SQL，'
              '步骤见 --mysql-guide；只想找 12345 只传 .MYD 即可')
    if bin_used and not args.count:
        tip = ('[说明] 含 MySQL 二进制表文件：编号 0x... 为字节偏移（非行号）；'
               '中文乱码可指定 --encoding gb18030')
        if not args.no_bin_number:
            tip += '；[数值 NB 小端] 为整数编码命中，可能误报，可用 --no-bin-number 关闭'
        print(tip)

    unit = '行/字节片段' if bin_used else '行'
    if args.count:
        print('命中 %s 处 / 扫描 %s %s / %d 个文件 / %.2fs'
              % (format(hit_count, ','), format(all_lines, ','), unit, len(paths), elapsed))
    else:
        print_hits(all_hits, matcher, args)
        print('\n命中 %s 处 / 扫描 %s %s / 读 %.1f MB / %d 个文件 / %.2fs / 编码 %s'
              % (format(hit_count, ','), format(all_lines, ','), unit, all_bytes / 1048576,
                 len(paths), elapsed, ','.join(sorted(enc_used))))
    if args.limit and hit_count >= args.limit:
        print('[提示] 已达命中上限 %d，可调高 --limit 或设 0 不限%s'
              % (args.limit, '；断点模式下 --resume 可接着查' if ckpt_mode else ''))
    export_hits(all_hits, args)
    return 0


if __name__ == '__main__':
    sys.exit(main())
