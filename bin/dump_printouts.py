#!/usr/bin/env python3

from collections import defaultdict
from functools import lru_cache
import os

from sqlalchemy import select, distinct

import yattag
from yattag import Doc

import pdg

ITEM_TYPES = {
    'P': 'specific',
    'A': '"also" alias',
    'W': '"was" alias',
    'S': 'shortcut',
    'B': 'both charges',
    'C': 'both charges, conjugate',
    'G': 'generic',
    'L': 'list',
    'I': 'inclusive',
    'T': 'text'
}


def get_sort(api, conn, pdgid):
    pdgid_table = api.db.tables['pdgid']
    query = select(pdgid_table.c.sort).where(pdgid_table.c.pdgid == pdgid)
    return conn.execute(query).scalar()


def get_charge_for_sorting(api, conn, item):
    pdgparticle_table = api.db.tables['pdgparticle']
    pdgitem_map_table = api.db.tables['pdgitem_map']
    pdgitem_table = api.db.tables['pdgitem']
    query = select(pdgparticle_table).where(pdgparticle_table.c.pdgitem_id == item)
    rows = conn.execute(query).fetchall()
    assert len(rows) in [0, 1]
    if rows:
        return rows[0].charge
    query = select(pdgitem_map_table).where(pdgitem_map_table.c.pdgitem_id == item)
    rows = conn.execute(query).fetchall()
    if len(rows) != 1:
        query = select(pdgitem_table).where(pdgitem_table.c.id == item)
        row = conn.execute(query).fetchone()
        print(f'Warning: Could not resolve charge of {item} {row.name}')
        return -10
    # assert len(rows) == 1
    return get_charge_for_sorting(api, conn, rows[0].target_id)


def get_sort_for_sorting(api, conn, item):
    pdgparticle_table = api.db.tables['pdgparticle']
    pdgitem_map_table = api.db.tables['pdgitem_map']
    pdgitem_table = api.db.tables['pdgitem']
    query = select(pdgparticle_table).where(pdgparticle_table.c.pdgitem_id == item)
    rows = conn.execute(query).fetchall()
    assert len(rows) in [0, 1]
    if rows:
        return get_sort(api, conn, rows[0].pdgid)
    query = select(pdgitem_map_table).where(pdgitem_map_table.c.pdgitem_id == item)
    rows = conn.execute(query).fetchall()
    if len(rows) == 0:
        query = select(pdgitem_table).where(pdgitem_table.c.id == item)
        row = conn.execute(query).fetchone()
        print(f'Warning: Could not resolve sort of {item} {row.name}')
        return 9999999999
    # assert len(rows) == 1
    return get_sort_for_sorting(api, conn, rows[0].target_id)


def item2items(api, conn, item):
    "Returns a generator of PDGITEMs that refer to the provided one"
    pdgitem_map_table = api.db.tables['pdgitem_map']

    query = select(pdgitem_map_table).where(pdgitem_map_table.c.target_id == item)
    for row in conn.execute(query).fetchall():
        yield row.pdgitem_id
        yield from item2items(api, conn, row.pdgitem_id)


def item2targets(api, conn, item):
    "Returns a generator of PDGITEMs that the provided one refers to"
    pdgitem_map_table = api.db.tables['pdgitem_map']

    query = select(pdgitem_map_table).where(pdgitem_map_table.c.pdgitem_id == item)
    for row in conn.execute(query).fetchall():
        yield row.target_id
        yield from item2targets(api, conn, row.target_id)

def pdgid2items(api, conn, pdgid):
    "Returns the set of all PDGITEMs that refer to the provided PDGID"
    pdgparticle_table = api.db.tables['pdgparticle']
    # pdgitem_table = api.db.tables['pdgitem']
    # pdgitem_map_table = api.db.tables['pdgitem_map']

    items = set()

    query = select(pdgparticle_table).where(pdgparticle_table.c.pdgid == pdgid)
    for row in conn.execute(query).fetchall():
        items.add(row.pdgitem_id)

        for item in item2items(api, conn, row.pdgitem_id):
            items.add(item)

            for tgt in item2targets(api, conn, item):
                items.add(tgt)

    return items


def item2pdgids(api, conn, item):
    "Returns a set of all of the PDGIDs referred to by the provided PDGITEM"
    pdgparticle_table = api.db.tables['pdgparticle']
    pdgitem_map_table = api.db.tables['pdgitem_map']

    query = select(pdgparticle_table).where(pdgparticle_table.c.pdgitem_id == item)
    rows = conn.execute(query).fetchall()
    assert len(rows) <= 1
    for row in rows:
        yield row.pdgid

    query = select(pdgitem_map_table).where(pdgitem_map_table.c.pdgitem_id == item)
    for row in conn.execute(query).fetchall():
        yield from item2pdgids(api, conn, row.target_id)


def pdgid2pdgids(api, conn, pdgid):
    """Returns a set of all PDGIDs (including the provided one) that have a
    PDGITEM in common with the provided PDGID"""
    pdgids = set()

    for item in pdgid2items(api, conn, pdgid):
        for pdgid in item2pdgids(api, conn, item):
            pdgids.add(pdgid)

    return pdgids


def all_pdgid_groups(api, conn):
    "Returns all sets of PDGIDs connected by common PDGITEMs (see pdgid2pdgids)"
    pdgparticle_table = api.db.tables['pdgparticle']

    seen_pdgids = set()
    groups = []

    query = select(distinct(pdgparticle_table.c.pdgid))
    for row in conn.execute(query).fetchall():
        if row.pdgid not in seen_pdgids:
            group = pdgid2pdgids(api, conn, row.pdgid)
            groups.append(group)
            for pdgid in group:
                seen_pdgids.add(pdgid)

    return groups


def group2items(api, conn, pdgids):
    "Returns all PDGITEMs associated with a set of PDGIDs"
    items = set()
    for pdgid in pdgids:
        for item in pdgid2items(api, conn, pdgid):
            items.add(item)

    return items


def get_item_data(api, conn, items):
    pdgitem_table = api.db.tables['pdgitem']
    pdgparticle_table = api.db.tables['pdgparticle']
    query = select(pdgitem_table, pdgparticle_table) \
        .join(pdgparticle_table,
              pdgitem_table.c.id == pdgparticle_table.c.pdgitem_id,
              isouter=True) \
        .where(pdgitem_table.c.id.in_(list(items))) \
        .order_by(pdgparticle_table.c.pdgid) # XXX
        # .order_by(pdgitem_table.c.id)
    return conn.execute(query).fetchall()


def item_data_for_group(api, conn, pdgids):
    items = group2items(api, conn, pdgids)

    return get_item_data(api, conn, items)


def html_helpers(doc):
    def key(name, klass=''):
        return doc.line('span', f'{name}: ', klass=f'key {klass}')

    def value(val, klass=''):
        return doc.line('span', val, klass=f'value {klass}')

    def pair(k, v, extra=None, klass=''):
        with doc.tag('span', klass='pair'):
            key(k, klass=klass)
            value(v, klass=klass)
            if extra:
                doc.line('span', ' ' + extra, klass=f'extra {klass}')

    def pairs():
        return doc.tag('div', klass='pairs')

    return pair, pairs


def dump_item_old(api, conn, doc, row):
    pdgitem_table = api.db.tables['pdgitem']
    pdgitem_map_table = api.db.tables['pdgitem_map']

    pair, pairs = html_helpers(doc)

    with pairs():
        pair('Name', row.name)
        pair('Item type', row.item_type,
             extra=f'({ITEM_TYPES[row.item_type]})')
        if row.pdgid:
            pair('PDGID', row.pdgid)
        if row.mcid:
            pair('MCID', row.mcid)

        query = select(pdgitem_map_table, pdgitem_table) \
            .join(pdgitem_table,
                  pdgitem_map_table.c.target_id == pdgitem_table.c.id) \
            .where(pdgitem_map_table.c.pdgitem_id == row.id)
        targets = conn.execute(query).fetchall()
        if targets:
            klass = 'suspect' if row.item_type == 'P' else ''
            pair('Targets', ', '.join(t.pdgitem_name for t in targets),
                 klass=klass)

    # return yattag.indent(doc.getvalue()) + '\n'

def item_type2klass(item_type):
    if item_type == 'P':
        return 'specific-type'
    if item_type in 'AWS':
        return 'alias-type'
    if item_type in 'BCG':
        return 'generic-type'
    return 'other-type'


def dump_item(api, conn, doc, row):
    pdgitem_table = api.db.tables['pdgitem']
    pdgitem_map_table = api.db.tables['pdgitem_map']

    _, tag, text, line = doc.ttl()

    def maybe(v):
        return v if (v is not None) else ''

    line('td', row.name)
    #line('td', row.item_type)
    doc.asis(f'<td>{row.item_type} <span class="extra">({ITEM_TYPES[row.item_type]})</span>')
    line('td', maybe(row.pdgid))
    line('td', maybe(row.mcid))
    line('td', maybe(row.charge))
    line('td', maybe(row.quantum_i))
    line('td', maybe(row.quantum_g))
    line('td', maybe(row.quantum_j))
    line('td', maybe(row.quantum_p))
    line('td', maybe(row.quantum_c))

    query = select(pdgitem_map_table, pdgitem_table) \
        .join(pdgitem_table,
                pdgitem_map_table.c.target_id == pdgitem_table.c.id) \
        .where(pdgitem_map_table.c.pdgitem_id == row.id)
    targets = conn.execute(query).fetchall()
    targets.sort(key=lambda t: get_charge_for_sorting(api, conn, t.target_id), reverse=True)
    if targets:
        # klass = 'suspect' if row.item_type == 'P' else ''
        with tag('td'):
            lines = [f'<span class={item_type2klass(t.item_type)}>{t.pdgitem_name}</span>'
                     for t in targets]
            doc.asis('<br>'.join(lines))
        # line('td', '\n'.join(t.pdgitem_name for t in targets), klass=klass)
    else:
        line('td', '')


def describe_pdgids(api, conn, pdgids):
    pdgparticle_table = api.db.tables['pdgparticle']

    parts = []

    for pdgid in pdgids:
        query = select(pdgparticle_table.c.name) \
            .where(pdgparticle_table.c.pdgid == pdgid)
        rows = conn.execute(query).fetchall()
        assert len(rows) > 0
        if len(rows) == 1:
            descrip = rows[0].name
        elif len(rows) == 2 and len(rows[1].name) <= 5:
            descrip = f'{rows[0].name}, {rows[1].name}'
        else:
            descrip = f'{rows[0].name}, ...'
        parts.append(f'{pdgid} ({descrip})')

    return ', '.join(parts)


def dump_group(api, conn, pdgids):
    item_data = item_data_for_group(api, conn, pdgids)
    # print(item_data)
    item_data.sort(key=lambda r: get_sort_for_sorting(api, conn, r[0]))

    generic_items = [r for r in item_data if r.item_type in 'G']
    generic_items += [r for r in item_data if r.item_type in 'B']
    generic_items += [r for r in item_data if r.item_type in 'C']
    # generic_items = [r for r in item_data if r.item_type in 'GBC']
    alias_items = [r for r in item_data if r.item_type in 'AWS']
    specific_items = [r for r in item_data if r.item_type == 'P']
    other_items = [r for r in item_data if r.item_type in 'LIT']

    # html = ''

    # for item_type in 'PAWSBCGLT':
    #     for row in item_data:
    #         if row.item_type == item_type:
    #             html += dump_item(api, conn, row)

    doc, tag, text, line = Doc().ttl()

    dumped = set()

    with tag('table'):
        with tag('thead'):
            with tag('tr'):
                line('th', 'Name')
                line('th', 'Item type')
                line('th', 'PDGID')
                line('th', 'MCID')
                line('th', 'Q')
                line('th', 'I')
                line('th', 'G')
                line('th', 'J')
                line('th', 'P')
                line('th', 'C')
                line('th', 'Mapped targets')
        with tag('tbody'):
            for row in generic_items:
                with tag('tr'):
                    dump_item(api, conn, doc, row)
                    dumped.add(row.pdgitem_name)
                # NB: PDGITEM.PDGITEM_ID is row[0] (if we do .pdgitem_id we get the possibly NULL joined value from pdgparticle)
                # print('<-', row[0], row.pdgitem_name)
                targets = list(item2targets(api, conn, row[0]))
                targets.sort(key=lambda t: get_charge_for_sorting(api, conn, t), reverse=True)
                for target in targets:
                    # print(target)
                    try:
                        t = next(r for r in item_data if r[0] == target)
                    except StopIteration:
                        # WHY IS THIS HAPPENING?!??!!
                        print(f"Need to look up {target} from {row[0]}")
                        # continue
                        t = get_item_data(api, conn, [target])[0]
                    # print('->', t)
                    if t.pdgitem_name not in dumped:
                        with tag('tr'):
                            dump_item(api, conn, doc, t)
                        dumped.add(t.pdgitem_name)
            for row in alias_items:
                with tag('tr'):
                    dump_item(api, conn, doc, row)
                    dumped.add(row.pdgitem_name)
                targets = list(item2targets(api, conn, row[0]))
                # targets.sort(key=lambda t: get_charge_for_sorting(api, conn, t), reverse=True)
                for target in targets:
                    t = next(r for r in item_data if r[0] == target)
                    if t.pdgitem_name not in dumped:
                        with tag('tr'):
                            dump_item(api, conn, doc, t)
                        dumped.add(t.pdgitem_name)
            for row in specific_items + other_items:
                if row.pdgitem_name not in dumped:
                    with tag('tr'):
                        dump_item(api, conn, doc, row)

            # with tag('tr'):
            #     dump_item(api, conn, doc, row)

    # for row in item_data:
    #     dump_item(api, conn, doc, row)

    # return html
    return yattag.indent(doc.getvalue()) + '\n'

def dump_page(api, conn, category, pdgids):
    doc, tag, text, line = Doc().ttl()
    stag = doc.stag

    doc.asis('<!DOCTYPE html>')

    with tag('html'):
        with tag('head'):
            stag('meta', charset='UTF-8')
            # stag('link', rel='stylesheet', href='printout.css')
            with tag('style'):
                css_path = os.path.dirname(__file__) + '/../etc/printout.css'
                text('\n' + open(css_path).read())
            # line('title', ', '.join(pdgids))
            line('title', category)
            # with tag('script', src='https://polyfill.io/v3/polyfill.min.js?features=es6'):
            #     pass
            # with tag('script', 'async', id='MathJax-script',
            #          src='https://cdn.jsdelivr.net/npm/mathjax@3/es5/tex-mml-chtml.js'):
            #     pass

        with tag('body'):
            # descrip = describe_pdgids(api, conn, pdgids)
            # line('div', f'Names for {descrip}', klass='title')
            line('div', category, klass='title')
            doc.asis('<div class="legend">Mapping legend: &nbsp;<b><span class="specific-type">Specific,</span>&nbsp; <span class="alias-type">alias,</span>&nbsp; <span class="generic-type">generic,</span>&nbsp; <span class="other-type">other</span></div>')
            doc.asis(dump_group(api, conn, pdgids))

    return yattag.indent(doc.getvalue()) + '\n'


@lru_cache
def _pdgid2category(api, conn):
    result = {}
    pdgparticle_table = api.db.tables['pdgparticle']
    query = select(pdgparticle_table)
    rows = conn.execute(query).fetchall()
    for row in rows:
        category = get_category(row.name)
        if row.pdgid in result:
            assert result[row.pdgid] == category
        else:
            result[row.pdgid] = category
    return result



def pdgid2category(api, conn, pdgid):
    return _pdgid2category(api, conn)[pdgid]


def get_metagroups(api, conn, groups):
    metagroups = defaultdict(lambda: [])
    for group in groups:
        for pdgid in group:
            category = pdgid2category(api, conn, pdgid)
            metagroups[category].append(pdgid)
    return metagroups


def dump_all(api, conn):
    groups = all_pdgid_groups(api, conn)
    metagroups = get_metagroups(api, conn, groups)

    os.mkdir('printouts')

    for category, group in metagroups.items():
        # group.sort(key=lambda pdgid: get_sort(api, conn, pdgid))
        name = category.replace(' ', '_').replace('/', '_').replace("'", '_prime')
        html = dump_page(api, conn, category, group)
        open(f'printouts/{name}.html', 'w').write(html)


def is_gauge_or_higgs(name):
    return name in ['gamma', 'g', 'graviton', 'W+', 'W-', 'Z0', 'H0']

def is_lepton(name):
    return name in ['e-', 'e+', 'mu-', 'mu+', 'tau-', 'tau+']

def is_quark(name):
    quarks = 'udscbt'
    return any(name == q or name == q + 'bar'
               for q in quarks)

def member(name: str, multiplet_name: str):
    # strip anything in parens
    if (p := name.find('(')) != -1:
        name = name[:p] + name[name.find(')')+1:]
    suffixes = ['', '0', 'bar', 'bar0', '0bar', '-', '+', 'bar-', 'bar+']
    # suffixes = ['']
    # return any(name.startswith(multiplet_name + suffix)
    #            for suffix in suffixes)
    return name.startswith(multiplet_name)

def is_unflavored_meson(name):
    if (name.find('_c') != -1) or (name.find('_b') != -1):
        return False
    multiplets = ['pi', 'eta', 'eta^\'', 'rho', 'omega', 'phi', 'a_', 'b_', 'f_', 'h_']
    return any(member(name, m) for m in multiplets)

def is_pion(name):
    return is_unflavored_meson(name) and member(name, 'pi')

def is_eta(name):
    return is_unflavored_meson(name) and (member(name, 'eta') or member(name, 'eta^'))

def is_rho(name):
    return is_unflavored_meson(name) and member(name, 'rho')

def is_omega(name):
    return is_unflavored_meson(name) and member(name, 'omega')

def is_phi(name):
    return is_unflavored_meson(name) and member(name, 'phi')

def is_a(name):
    return is_unflavored_meson(name) and member(name, 'a_')

def is_b(name):
    return is_unflavored_meson(name) and member(name, 'b_')

def is_f(name):
    return is_unflavored_meson(name) and member(name, 'f_')

def is_h(name):
    return is_unflavored_meson(name) and member(name, 'h_')

def is_strange_meson(name):
    return name.startswith('K')

def is_charmed_meson(name):
    return name.startswith('D') and not name.startswith('Delta')

def is_bottom_meson(name):
    return name.startswith('B')

def is_charmonium(name):
    multiplets = ['eta_c', 'J/psi', 'psi', 'chi_c', 'h_c']
    return any(member(name, m) for m in multiplets)

def is_bottomonium(name):
    multiplets = ['Upsilon', 'eta_b', 'chi_b', 'h_b', 'chi_b']
    return any(member(name, m) for m in multiplets)

def is_N_baryon(name):
    multiplets = ['p', 'n', 'N']
    return any(member(name, m) for m in multiplets) \
        and not (name.startswith('pi') or name.startswith('phi'))

def is_delta_baryon(name):
    return member(name, 'Delta')

def is_lambda_baryon(name):
    return member(name, 'Lambda')

def is_sigma_baryon(name):
    return member(name, 'Sigma')

def is_xi_baryon(name):
    return member(name, 'Xi')

def is_omega_baryon(name):
    return member(name, 'Omega')

def is_tetra_penta(name):
    return name.startswith('P') or name.startswith('T')

def is_unclassified(name):
    return any(name.startswith(c) for c in 'RXYZ') \
        and name != 'Z0' and not name.startswith('Xi')

def get_category(name):
    if is_gauge_or_higgs(name):
        return 'Gauge/Higgs bosons'
    if is_lepton(name):
        return 'Leptons'
    if is_quark(name):
        return 'Quarks'
    if is_pion(name):
        return 'Pions'
    if is_eta(name):
        return 'Light eta mesons'
    if is_rho(name):
        return 'Rho mesons'
    if is_omega(name):
        return 'Omega mesons'
    if is_phi(name):
        return 'Phi mesons'
    if is_a(name):
        return 'a mesons'
    if is_b(name):
        return 'b mesons'
    if is_f(name):
        return 'f mesons'
    if is_h(name):
        return 'Light h mesons'
    if is_strange_meson(name):
        return 'Strange mesons'
    if is_charmed_meson(name):
        return 'Charmed mesons'
    if is_bottom_meson(name):
        return 'Bottom mesons'
    if is_charmonium(name):
        return 'Charmonia'
    if is_bottomonium(name):
        return 'Bottomonia'
    if is_N_baryon(name):
        return 'N baryons'
    if is_delta_baryon(name):
        return 'Delta baryons'
    if is_lambda_baryon(name):
        return 'Lambda baryons'
    if is_sigma_baryon(name):
        return 'Sigma baryons'
    if is_xi_baryon(name):
        return 'Xi baryons'
    if is_omega_baryon(name):
        return 'Omega baryons'
    if is_sigma_baryon(name):
        return 'Sigma baryons'
    if is_tetra_penta(name):
        if EDITION == '2023':
            return 'Pentaquarks'
        else:
            return 'Pentaquarks and tetraquarks'
    if is_unclassified(name):
        return 'Other mesons'
    return 'Error'


def get_edition(api, conn):
    pdginfo = api.db.tables['pdginfo']
    q = select(pdginfo.c.value).where(pdginfo.c.name == "edition")
    return conn.execute(q).fetchone()[0]


if __name__ == '__main__':
    import pdg
    api = pdg.connect()
    conn = api.engine.connect()
    global EDITION
    EDITION = get_edition(api, conn)
    dump_all(api, conn)
