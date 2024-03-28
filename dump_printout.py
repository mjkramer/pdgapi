#!/usr/bin/env python3

import argparse

from sqlalchemy import select, distinct

import yattag
from yattag import Doc

import pdg



def get_props(cls):
    return [p for p in dir(cls)
            if type(getattr(cls, p)) is property]

def dump_particle(pdgid):
    doc, tag, text, line = Doc().ttl()

    doc.asis('<!DOCTYPE html>')


def display_article(data):

    doc, tag, text, line = Doc(
        defaults = data['form_defaults'],
        errors = data['form_errors']
    ).ttl()

    doc.asis('<!DOCTYPE html>')

    with tag('html'):

        with tag('body'):

            line('h1', data['article']['title'])

            with tag('div', klass = 'description'):
                text(data['article']['description'])

            with tag('form', action = '/add-to-cart'):
                # doc.input(name = 'article_id', type = 'hidden')
                # doc.input(name = 'quantity', type = 'text')

                doc.stag('input', type = 'submit', value = 'Add to cart')

    return doc.getvalue()


def mock_up():
    doc, tag, text, line = Doc().ttl()
    stag = doc.stag

    def pairs():
        return tag('div', klass='pairs')

    def key(name):
        return line('span', f'{name}: ', klass='key')

    def value(val):
        return line('span', val, klass='value')

    def pair(k, v):
        with tag('div', klass='pair'):
            key(k)
            value(v)

    doc.asis('<!DOCTYPE html>')

    """
<script src="https://polyfill.io/v3/polyfill.min.js?features=es6"></script>
<script id="MathJax-script" async src="https://cdn.jsdelivr.net/npm/mathjax@3/es5/tex-mml-chtml.js"></script>
    """

    with tag('html'):
        with tag('head'):
            stag('meta', charset='UTF-8')
            # stag('link', rel='stylesheet', href='printout.css')
            with tag('style'):
                text('\n' + open('printout.css').read())
            line('title', 'PdgParticle pi+')
            with tag('script', src='https://polyfill.io/v3/polyfill.min.js?features=es6'):
                pass
            with tag('script', 'async', id='MathJax-script',
                 src='https://cdn.jsdelivr.net/npm/mathjax@3/es5/tex-mml-chtml.js'):
                pass
        with tag('body'):
            title = 'PdgParticle pi+ (\\(\\pi^+\\))'
            line('div', title, klass='title')

            with pairs():
                pair('MC ID', '211')
                pair('PDG ID', 'S008')
                pair('Item type', 'P')

            line('div', 'Unique aliases', klass='section')

            with pairs():
                pair('Name', 'K(s) (\\(K_s\\))')
                pair('Item type', 'S')

            line('div', 'Generic aliases', klass='section')

            with pairs():
                pair('Name', 'pi+- (\\(\\pi^\\pm\\))')
                pair('Item type', 'B')

            with pairs():
                pair('Name', 'pi (\\(\\pi\\))')
                pair('Item type', 'G')

    return doc.getvalue()


def dump_item_txt(api, conn, pdgitem_id):
    pdgitem_table = api.db.tables['pdgitem']
    print(f'{pdgitem_id = }')
    query = select(pdgitem_table.c.name, pdgitem_table.c.name_tex,
                   pdgitem_table.c.item_type) \
        .where(pdgitem_table.c.id == pdgitem_id)
    rows = conn.execute(query).fetchall()
    assert len(rows) == 1
    for row in rows:
        print(f'{row.name = }')
        print(f'{row.name_tex = }')
        print(f'{row.item_type = }')
        print()


# TODO: Check names are equal
def dump_generic_txt(api, conn, pdgitem_id, indent=''):
    pdgitem_table = api.db.tables['pdgitem']
    pdgitem_map_table = api.db.tables['pdgitem_map']
    query = select(pdgitem_table, pdgitem_map_table) \
        .join(pdgitem_map_table, pdgitem_map_table.c.pdgitem_id == pdgitem_table.c.id) \
        .where((pdgitem_map_table.c.target_id == pdgitem_id) &
               (pdgitem_table.c.item_type.not_in(['A', 'W', 'S']))) \
        .order_by(pdgitem_map_table.c.sort)
    for row in conn.execute(query).fetchall():
        print(f'{indent}{row.pdgitem_map_name = }')
        print(f'{indent}{row.pdgitem_name = }')
        print(f'{indent}{row.item_type = }')
        print()
        dump_generic(api, conn, row.pdgitem_id, indent + '    ')


# TODO: Check names are equal
def dump_unique_txt(api, conn, pdgitem_id, indent=''):
    pdgitem_table = api.db.tables['pdgitem']
    pdgitem_map_table = api.db.tables['pdgitem_map']
    query = select(pdgitem_table, pdgitem_map_table) \
        .join(pdgitem_map_table, pdgitem_map_table.c.pdgitem_id == pdgitem_table.c.id) \
        .where((pdgitem_map_table.c.target_id == pdgitem_id) &
               (pdgitem_table.c.item_type.in_(['A', 'W', 'S']))) \
        .order_by(pdgitem_map_table.c.sort)
    for row in conn.execute(query).fetchall():
        print(f'{indent}{row.pdgitem_map_name = }')
        print(f'{indent}{row.pdgitem_name = }')
        print(f'{indent}{row.item_type = }')
        print()
        dump_unique(api, conn, row.pdgitem_id, indent + '    ')


def dump_particles_txt(api, conn, pdgid):
    pdgparticle_table = api.db.tables['pdgparticle']
    print(f'{pdgid = }\n')
    query = select(pdgparticle_table.c.name, pdgparticle_table.c.mcid,
                   pdgparticle_table.c.pdgitem_id) \
        .where(pdgparticle_table.c.pdgid == pdgid)
    for row in conn.execute(query).fetchall():
        print(f'{row.name = }')
        print(f'{row.mcid = }')
        print()
        dump_item_txt(api, conn, row.pdgitem_id)
        dump_unique_txt(api, conn, row.pdgitem_id)
        dump_generic_txt(api, conn, row.pdgitem_id)

def wraptex(name_tex):
    if name_tex in [None, '']:
        # return 'no TeX name'
        return '\\(\\text{no TeX name}\\)'
    else:
        assert name_tex[0] == '$' and name_tex[-1] == '$'
        return f'\\({name_tex[1:-1]}\\)'


def dump_particle(api, conn, name):
    pdgparticle_table = api.db.tables['pdgparticle']
    pdgitem_table = api.db.tables['pdgitem']
    pdgitem_map_table = api.db.tables['pdgitem_map']

    query = select(pdgparticle_table) \
        .where(pdgparticle_table.c.name == name)
    rows = conn.execute(query).fetchall()
    assert len(rows) == 1
    pdgparticle_row = rows[0]

    query = select(pdgitem_table) \
        .where(pdgitem_table.c.id == pdgparticle_row.pdgitem_id)
    rows = conn.execute(query).fetchall()
    assert len(rows) == 1
    pdgitem_row = rows[0]
    assert pdgitem_row.name == pdgparticle_row.name

    def get_aliases(target_id, unique=True):
        if unique:
            f = pdgitem_table.c.item_type.in_
        else:
            f = pdgitem_table.c.item_type.not_in
        pred = f(['A', 'W', 'S'])

        query = select(pdgitem_table, pdgitem_map_table) \
            .join(pdgitem_map_table,
                  pdgitem_map_table.c.pdgitem_id == pdgitem_table.c.id) \
            .where(pred & (pdgitem_map_table.c.target_id == target_id)) \
            .order_by(pdgitem_map_table.c.sort)
        # query = select(pdgitem_map_table) \
        #     .where(pred & (pdgitem_map_table.c.target_id == target_id)) \
        #     .order_by(pdgitem_map_table.c.sort)

        rows = conn.execute(query).fetchall()
        for row in rows:
            assert len(get_aliases(row.pdgitem_map_pdgitem_id, True)) == 0
            assert len(get_aliases(row.pdgitem_map_pdgitem_id, False)) == 0

        return rows

    doc, tag, text, line = Doc().ttl()
    stag = doc.stag

    def pairs():
        return tag('div', klass='pairs')

    def key(name):
        return line('span', f'{name}: ', klass='key')

    def value(val):
        return line('span', val, klass='value')

    def pair(k, v):
        with tag('div', klass='pair'):
            key(k)
            value(v)

    doc.asis('<!DOCTYPE html>')

    """
<script src="https://polyfill.io/v3/polyfill.min.js?features=es6"></script>
<script id="MathJax-script" async src="https://cdn.jsdelivr.net/npm/mathjax@3/es5/tex-mml-chtml.js"></script>
    """

    with tag('html'):
        with tag('head'):
            stag('meta', charset='UTF-8')
            # stag('link', rel='stylesheet', href='printout.css')
            with tag('style'):
                text('\n' + open('printout.css').read())
            line('title', f'PdgParticle {name}')
            with tag('script', src='https://polyfill.io/v3/polyfill.min.js?features=es6'):
                pass
            with tag('script', 'async', id='MathJax-script',
                     src='https://cdn.jsdelivr.net/npm/mathjax@3/es5/tex-mml-chtml.js'):
                pass

        with tag('body'):
            title = f'PdgParticle {name} ({wraptex(pdgitem_row.name_tex)})'
            line('div', title, klass='title')

            with pairs():
                pair('MCID', str(pdgparticle_row.mcid))
                pair('PDGID', str(pdgparticle_row.pdgid))
                pair('Item type', str(pdgitem_row.item_type))

            for label, unique in [('Unique', True), ('Generic', False)]:
                line('div', f'{label} aliases', klass='section')

                for row in get_aliases(pdgitem_row.id, unique=unique):
                    with pairs():
                        pair('Name', f'{row.pdgitem_name} ({wraptex(row.pdgitem_name_tex)})')
                        pair('Item type', str(row.pdgitem_item_type))

    return doc.getvalue()


def item2items(api, conn, item):
    pdgitem_map_table = api.db.tables['pdgitem_map']

    query = select(pdgitem_map_table).where(pdgitem_map_table.c.target_id == item)
    for row in conn.execute(query).fetchall():
        yield row.pdgitem_id
        yield from item2items(api, conn, row.pdgitem_id)


def pdgid2items(api, conn, pdgid):
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
    pdgids = set()

    for item in pdgid2items(api, conn, pdgid):
        for pdgid in item2pdgids(api, conn, item):
            pdgids.add(pdgid)

    return pdgids


def all_pdgid_groups(api, conn):
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
    def key(name):
        return doc.line('span', f'{name}: ', klass='key')

    def value(val):
        return doc.line('span', val, klass='value')

    def pair(k, v):
        with doc.tag('div', klass='pair'):
            key(k)
            value(v)

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
        pair('Item type', row.item_type)
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
            pair('Targets', ', '.join(t.pdgitem_name for t in targets))

    return yattag.indent(doc.getvalue()) + '\n'


def format_title(api, conn, pdgids):
    pdgparticle_table = api.db.tables['pdgparticle']

    parts = []

    for pdgid in pdgids:
        query = select(pdgparticle_table.c.name) \
            .where(pdgparticle_table.c.pdgid == pdgid)
        rows = conn.execute(query).fetchall()
        assert len(rows) > 0
        if len(rows) == 1:
            descrip = rows[0].name
        elif len(rows) == 2 and len(rows[1]) <= 5:
            descrip = f'{rows[0].name}, {rows[1].name}'
        else:
            descrip = f'{rows[0].name}, ...'
        parts.append(f'{pdgid} ({descrip})')

    return ', '.join(parts)


def dump_group(api, conn, pdgids):
    item_data = item_data_for_group(api, conn, pdgids)

    html = ''

    def dump_for_cond(cond):
        nonlocal html
        for row in item_data:
            if cond(row):
                html += dump_item(api, conn, row)

    dump_for_cond(lambda row: row.item_type == 'P')
    dump_for_cond(lambda row: row.item_type in ['A', 'W', 'S'])
    dump_for_cond(lambda row: row.item_type in ['G', 'B'])
    dump_for_cond(lambda row: row.item_type not in ['P', 'A', 'W', 'S', 'G', 'B'])

    return html

if __name__ == '__main0__':
    import pdg
    from dump_printout import *
    api = pdg.connect()
    conn = api.engine.connect()
    print(dump_item(api, conn, get_item_data(api, conn, [5567])[0]))
    for g in all_pdgid_groups(api, conn):
        print(g)
        for row in item_data_for_group(api, conn, g):
            print(row)
        print()
    # dump_particles(api, conn, 'S004') # muon
    # dump_particles(api, conn, 'S008') # charged pion
    # dump_particles(api, conn, 'S012') # K(S)0


if __name__ == '__main1__':
    html = mock_up()
    open('mock_up.html', 'w').write(yattag.indent(html) + '\n')


if __name__ == '__main2__':
    import pdg
    from dump_printout import *
    api = pdg.connect()
    conn = api.engine.connect()

    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument('name')
    args = ap.parse_args()

    html = dump_particle(api, conn, args.name)
    html = yattag.indent(html) + '\n'
    open(f'pdgparticle_{args.name}.html', 'w').write(html)



if __name__ == '__main__':
    import traceback
    import os
    os.system('mkdir -p printouts')

    import pdg
    from dump_printout import *
    api = pdg.connect()
    conn = api.engine.connect()

    query = select(api.db.tables['pdgparticle'])
    for row in conn.execute(query).fetchall():
        print(row.name)
        try:
            html = dump_particle(api, conn, row.name)
        except Exception as e:
            traceback.print_exc()
            continue

        html = yattag.indent(html) + '\n'
        open(f'printouts/{row.name}.html', 'w').write(html)
