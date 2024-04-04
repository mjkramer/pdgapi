#!/usr/bin/env python3

from collections import defaultdict
from functools import lru_cache
import os

from sqlalchemy import select, distinct

import yattag
from yattag import Doc

import pdg

ITEM_TYPES = {
    'P': 'specific charge',
    'A': '"also" alias',
    'W': '"was" alias',
    'S': 'shortcut',
    'B': 'both charges',
    'C': 'both charges, conjugate',
    'G': 'generic',
    'L': 'list',
    'T': 'text'
}


def item2items(api, conn, item):
    "Returns a generator of PDGITEMs that refer to the provided one"
    pdgitem_map_table = api.db.tables['pdgitem_map']

    query = select(pdgitem_map_table).where(pdgitem_map_table.c.target_id == item)
    for row in conn.execute(query).fetchall():
        yield row.pdgitem_id
        yield from item2items(api, conn, row.pdgitem_id)


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
    query = select(pdgitem_table, pdgparticle_table.c.pdgid, pdgparticle_table.c.mcid) \
        .join(pdgparticle_table,
              pdgitem_table.c.id == pdgparticle_table.c.pdgitem_id,
              isouter=True) \
        .where(pdgitem_table.c.id.in_(list(items))) \
        .order_by(pdgitem_table.c.id)
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
        with doc.tag('div', klass='pair'):
            key(k, klass=klass)
            value(v, klass=klass)
            if extra:
                doc.line('span', ' ' + extra, klass=f'extra {klass}')

    def pairs():
        return doc.tag('div', klass='pairs')

    return pair, pairs


def dump_item(api, conn, row):
    pdgitem_table = api.db.tables['pdgitem']
    pdgitem_map_table = api.db.tables['pdgitem_map']

    doc = Doc()
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

    return yattag.indent(doc.getvalue()) + '\n'


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

    html = ''

    for item_type in 'PAWSBCGLT':
        for row in item_data:
            if row.item_type == item_type:
                html += dump_item(api, conn, row)

    return html

def dump_page(api, conn, pdgids):
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
            line('title', ', '.join(pdgids))
            # with tag('script', src='https://polyfill.io/v3/polyfill.min.js?features=es6'):
            #     pass
            # with tag('script', 'async', id='MathJax-script',
            #          src='https://cdn.jsdelivr.net/npm/mathjax@3/es5/tex-mml-chtml.js'):
            #     pass

        with tag('body'):
            # descrip = describe_pdgids(api, conn, pdgids)
            # line('div', f'Names for {descrip}', klass='title')
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
        group = sorted(group)
        name = category.replace(' ', '_').replace('/', '_')
        html = dump_page(api, conn, group)
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
    return any(name.startswith(multiplet_name + suffix)
               for suffix in suffixes)

def is_unflavored_meson(name):
    multiplets = ['pi', 'eta', 'eta^\'', 'rho', 'omega', 'phi', 'a_', 'b_', 'f_', 'h_']
    return any(member(name, m) for m in multiplets)

def is_strange_meson(name):
    return name.startswith('K')

def is_charmed_meson(name):
    return name.startswith('D')

def is_bottom_meson(name):
    return name.startswith('B')

def is_charmonium(name):
    multiplets = ['eta_c', 'J/psi', 'psi', 'chi_c', 'h_c']
    return any(member(name, m) for m in multiplets)

def is_bottomonium(name):
    multiplets = ['Upsilon', 'chi_b', 'h_b', 'chi_b']
    return any(member(name, m) for m in multiplets)

def is_N_baryon(name):
    multiplets = ['p', 'n', 'N']
    return any(member(name, m) for m in multiplets) \
        and not name.startswith('pi')

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

def is_exotic(name):
    return any(name.startswith(c) for c in 'PTRXYZ') \
        and name != 'Z0'

def get_category(name):
    if is_gauge_or_higgs(name):
        return 'Gauge/Higgs bosons'
    if is_lepton(name):
        return 'Leptons'
    if is_quark(name):
        return 'Quarks'
    if is_unflavored_meson(name):
        return 'Unflavored mesons'
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
    if is_exotic(name):
        return 'Exotic states'
    return 'Error'


if __name__ == '__main__':
    import pdg
    api = pdg.connect()
    conn = api.engine.connect()
    dump_all(api, conn)
