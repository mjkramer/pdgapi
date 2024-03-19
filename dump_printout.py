#!/usr/bin/env python3

import argparse

from sqlalchemy import select

import pdg
from yattag import Doc


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


def dump_item(api, conn, pdgitem_id):
    pdgitem_table = api.db.tables['pdgitem']
    print(pdgitem_id)
    query = select(pdgitem_table.c.name, pdgitem_table.c.name_tex,
                   pdgitem_table.c.item_type) \
        .where(pdgitem_table.c.id == pdgitem_id)
    rows = conn.execute(query).fetchall()
    assert len(rows) == 1
    for row in rows:
        print(row.name)
        print(row.name_tex)
        print(row.item_type)


def dump_unique(api, conn, pdgitem_id):
    pdgitem_table = api.db.tables['pdgitem']
    pdgitem_map_table = api.db.tables['pdgitem_map']
    query = select(pdgitem_table, pdgitem_map_table) \
        .join(pdgitem_map_table, pdgitem_map_table.c.pdgitem_id == pdgitem_table.c.id) \
        .where(pdgitem_map_table.c.target_id == pdgitem_id and
               pdgitem_table.c.item_type.in_(['A', 'W', 'S'])) \
        .order_by(pdgitem_map_table.c.sort)
    for row in conn.execute(query).fetchall():
        print(row.pdgitem_map_name)
        print(row.pdgitem_name)
        print(row.item_type)


def dump_particles(api, conn, pdgid):
    pdgparticle_table = api.db.tables['pdgparticle']
    print(pdgid)
    query = select(pdgparticle_table.c.name, pdgparticle_table.c.mcid,
                   pdgparticle_table.c.pdgitem_id) \
        .where(pdgparticle_table.c.pdgid == pdgid)
    for row in conn.execute(query).fetchall():
        print(row.name)
        print(row.mcid)
        dump_item(api, conn, row.pdgitem_id)
        dump_unique(api, conn, row.pdgitem_id)
        # dump_generic(api, conn, row.pdgitem_id)


if __name__ == '__main__':
    import pdg
    from dump_printout import *
    api = pdg.connect()
    conn = api.engine.connect()
    dump_particles(api, conn, 'S004')
