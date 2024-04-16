#!/usr/bin/env python3

from collections import Counter, defaultdict
from dataclasses import dataclass
from functools import lru_cache
import os
from typing import Iterable

from sqlalchemy import Row, select, distinct

import yattag
from yattag import Doc

import pdg

from _categories import get_category


ITEM_TYPES = {
    'P': 'specific',
    'A': '"also" alias',
    'W': '"was" alias',
    'S': 'shortcut',
    'B': 'both charges',
    'C': 'both, conjugate',
    'G': 'generic',
    'L': 'list',
    'I': 'inclusive',
    'T': 'text'
}

API = pdg.connect()
CONN = API.engine.connect()
execute = CONN.execute

PDGID = API.db.tables['pdgid']
PDGPARTICLE = API.db.tables['pdgparticle']
PDGITEM = API.db.tables['pdgitem']
PDGITEM_MAP = API.db.tables['pdgitem_map']
PDGINFO = API.db.tables['pdginfo']

EDITION = execute(select(PDGINFO.c.value)
                  .where(PDGINFO.c.name == "edition")).scalar()


def pdgid2sort(pdgid: str) -> int:
    q = select(PDGID.c.sort).where(PDGID.c.pdgid == pdgid)
    return execute(q).scalar()  # type: ignore


def item2pdgids(item: int) -> set[str]:
    def inner(item: int):
        q = select(PDGPARTICLE).where(PDGPARTICLE.c.pdgitem_id == item)
        rows = execute(q).fetchall()
        assert len(rows) <= 1
        if len(rows) == 1:
            yield rows[0].pdgid
        q = select(PDGITEM_MAP).where(PDGITEM_MAP.c.pdgitem_id == item)
        for row in execute(q).fetchall():
            yield from inner(row.target_id)
    return set(inner(item))


def item_type2klass(item_type):
    if item_type == 'P':
        return 'specific-type'
    if item_type in 'AWS':
        return 'alias-type'
    if item_type in 'BCG':
        return 'generic-type'
    return 'other-type'


def item_data(item: int) -> Row:
    q = select(PDGITEM, PDGPARTICLE) \
        .join(PDGPARTICLE,
                PDGITEM.c.id == PDGPARTICLE.c.pdgitem_id,
                isouter=True) \
        .where(PDGITEM.c.id == item)
    rows = execute(q).fetchall()
    assert len(rows) == 1
    return rows[0]


def maybe(v):
    return v if (v is not None) else ''


def render_item(row: Row) -> str:
    doc, tag, text, line = Doc().ttl()

    line('td', row.name)
    #line('td', row.item_type)
    with tag('td'):
        text(row.item_type)
        with tag('span', klass='extra'):
            text(ITEM_TYPES[row.item_type])

    line('td', maybe(row.pdgid))
    line('td', maybe(row.mcid))
    line('td', maybe(row.charge))
    line('td', maybe(row.quantum_i))
    line('td', maybe(row.quantum_g))
    line('td', maybe(row.quantum_j))
    line('td', maybe(row.quantum_p))
    line('td', maybe(row.quantum_c))

    q = select(PDGITEM_MAP, PDGITEM) \
        .join(PDGITEM,
              PDGITEM_MAP.c.target_id == PDGITEM.c.id) \
        .where(PDGITEM_MAP.c.pdgitem_id == row.id)
    targets = execute(q).fetchall()

    with tag('td'):
        for i, t in enumerate(targets):
            if i != 0:
                tag('br')
            with tag('span', klass=item_type2klass(t.item_type)):
                text(t.pdgitem_name)

    return doc.getvalue()


@dataclass
class ItemGroup:
    pdgids: set[str] = set()
    item_data: list[Row] = []

    def has_any(self, pdgids: Iterable[str]) -> bool:
        return any(pdgid in self.pdgids for pdgid in pdgids)

    @property
    def category(self) -> str:
        categories = (get_category(dat.name, EDITION) for dat in self.item_data)
        return Counter(categories).most_common(1)[0][0]

    @property
    def sort_order(self) -> int:
        return min(pdgid2sort(pdgid) for pdgid in self.pdgids)

    def update_pdgids(self, pdgids: Iterable[str]):
        for pdgid in pdgids:
            self.pdgids.add(pdgid)

    def add_item(self, item: int):
        self.item_data.append(item_data(item))

    @staticmethod
    def all() -> list['ItemGroup']:
        groups: list[ItemGroup] = []
        q = select(PDGITEM).where(PDGITEM.c.item_type.not_in(['L', 'I', 'T']))
        for row in execute(q).fetchall():
            pdgids = item2pdgids(row.id)
            try:
                g = next(g for g in groups if g.has_any(pdgids))
            except StopIteration:
                g = ItemGroup()
                groups.append(g)
            g.update_pdgids(pdgids)
            g.add_item(row.id)
        return groups

    @staticmethod
    def all_categorized() -> dict[str, list['ItemGroup']]:
        d = defaultdict(lambda: [])
        for group in ItemGroup.all():
            d[group.category].append(group)
        for category in d:
            d[category].sort(key=lambda g: g.sort_order)
        return d


    def arrange(self):
        self.item_data.sort(key=lambda row: row.charge, reverse=True)

        def items_typed(types: list[str]):
            return [it for it in self.item_data if it.item_type in types]

        aliases = items_typed(['A', 'W', 'S'])
        generics = items_typed(['G', 'B', 'C'])
        others = items_typed(['L', 'I', 'T'])
        specifics = items_typed(['P'])

        self.item_data = aliases + generics + others + specifics

    def render(self) -> str:
        doc = Doc()
        rendered = set()

        for row in self.item_data:
            # .pdgitem_id could(?) refer to PDGPARTICLE.pdgitem_id which might
            # be null since isouter=True. So use [0] i.e. PDGITEM.id.
            item = row[0]

            if item in rendered:
                continue
            rendered.add(item)

            doc.asis(render_item(row))

        return doc.getvalue()


def render_page(category: str, groups: list[ItemGroup]) -> str:
    doc, tag, text, line = Doc().ttl()
    stag = doc.stag

    doc.asis('<!DOCTYPE html>')

    with tag('html'):
        with tag('head'):
            stag('meta', charset='UTF-8')
            with tag('style'):
                css_path = os.path.dirname(__file__) + '/../etc/printout.css'
                text('\n' + open(css_path).read())
            line('title', category)

        with tag('body'):
            line('div', category, klass='title')
            with tag('div', klass='legend'):
                text('Mapping legend:')
                line('span', 'Specific, ', klass='legend-label specific-type')
                line('span', 'alias, ', klass='legend-label alias-type')
                line('span', 'generic, ', klass='legend-label generic-type')
                line('span', 'other', klass='legend-label other-type')
            for g in groups:
                doc.asis(g.render())

    return doc.getvalue()


def main():
    os.mkdir('printouts')

    for category, groups in ItemGroup.all_categorized().items():
        raw_html = render_page(category, groups)
        html = yattag.indent(raw_html) + '\n'
        name = category.replace(' ', '_').replace('/', '_').replace("'", '_prime')
        open(f'printouts/{name}.html', 'w').write(html)


if __name__ == '__main__':
    main()
