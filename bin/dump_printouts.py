#!/usr/bin/env python3

from collections import Counter, defaultdict
from functools import lru_cache
import os
from typing import Iterable

from sqlalchemy import Row, select

import yattag
from yattag import Doc

import pdg


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

# HACK; not needed as long as every (non-text) Item can be associated to a
# Particle(s)
PDGID_DICT = {
    # 'X(3915)': 'M159',
    # 'D_sJ^*(2860)': 'M196',     # QUESTIONABLE... M226?
    # 'R_c 0(4240)': 'M216',
    # 'X(4240)+-': 'M216',
}

API = pdg.connect()
CONN = API.engine.connect()
execute = CONN.execute

PDGID = API.db.tables['pdgid']
PDGPARTICLE = API.db.tables['pdgparticle']
PDGITEM = API.db.tables['pdgitem']
PDGITEM_MAP = API.db.tables['pdgitem_map']
PDGINFO = API.db.tables['pdginfo']

@lru_cache
def get_edition():
    return execute(select(PDGINFO.c.value)
                   .where(PDGINFO.c.name == "edition")).scalar()


def pdgid2sort(pdgid: str) -> int:
    q = select(PDGID.c.sort).where(PDGID.c.pdgid == pdgid)
    return execute(q).scalar()  # type: ignore


def item2pdgids_down(item: int, exclude = []) -> Iterable[str]:
    q = select(PDGPARTICLE).where(PDGPARTICLE.c.pdgitem_id == item)
    rows = execute(q).fetchall()
    assert len(rows) <= 1
    if len(rows) == 1:
        yield rows[0].pdgid
    q = select(PDGITEM_MAP).where(PDGITEM_MAP.c.pdgitem_id == item)
    for row in execute(q).fetchall():
        if row.target_id not in exclude:
            yield from item2pdgids_down(row.target_id)


# Not needed as long as every generic/alias item can be used to reach a particle
# by following its target(s)
def item2pdgids_up(item: int) -> Iterable[str]:
    q = select(PDGITEM_MAP).where(PDGITEM_MAP.c.target_id == item)
    rows = execute(q).fetchall()
    for row in rows:
        yield from item2pdgids_down(row.pdgitem_id, exclude=[item])
        yield from item2pdgids_up(row.pdgitem_id)


def item2pdgids(item: int):
    # return set(item2pdgids_down(item)) | set(item2pdgids_up(item))
    return set(item2pdgids_down(item))


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
    with tag('td', klass=item_type2klass(row.item_type)):
        text(row.item_type)
        with tag('span', klass='extra'):
            text(f' ({ITEM_TYPES[row.item_type]})')

    line('td', maybe(row.pdgid))
    line('td', maybe(row.mcid))
    line('td', maybe(row.charge))
    line('td', maybe(row.quantum_i))
    line('td', maybe(row.quantum_g))
    line('td', maybe(row.quantum_j))
    line('td', maybe(row.quantum_p))
    line('td', maybe(row.quantum_c))

    q = select(PDGITEM_MAP, PDGITEM, PDGPARTICLE) \
        .join(PDGITEM,
              PDGITEM_MAP.c.target_id == PDGITEM.c.id) \
        .join(PDGPARTICLE,
              PDGITEM_MAP.c.target_id == PDGPARTICLE.c.pdgitem_id,
              isouter=True) \
        .where(PDGITEM_MAP.c.pdgitem_id == row.id) \
        .order_by(PDGPARTICLE.c.charge.desc())
    targets = execute(q).fetchall()

    with tag('td'):
        for i, t in enumerate(targets):
            if i != 0:
                doc.stag('br')
            with tag('span', klass=item_type2klass(t.item_type)):
                text(t.pdgitem_name)

    return doc.getvalue()


class ItemGroup:
    def __init__(self):
        self.pdgids: set[str] = set()
        self.item_data: list[Row] = []

    def has_any(self, pdgids: Iterable[str]) -> bool:
        return any(pdgid in self.pdgids for pdgid in pdgids)

    @property
    def category(self) -> str:
        categories = (get_category(dat.name, get_edition())
                      for dat in self.item_data)
        return Counter(categories).most_common(1)[0][0]

    @property
    def sort_order(self) -> int:
        try:
            return min(pdgid2sort(pdgid) for pdgid in self.pdgids)
        except:
            print(f'Cannot determine PDGIDs:\n{self.item_data}\n')
            return -1000

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
            if not pdgids:
                print(f'LOOKING UP {row.name}... ', end='')
                try:
                    pdgids = {PDGID_DICT[row.name]}
                    print('found')
                except:
                    print('skipping')
                    continue
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
        def charge_or_default(row):
            return row.charge if (row.charge is not None) else -1000
        self.item_data.sort(key=charge_or_default, reverse=True)

        def items_typed(types: list[str]):
            return [it for it in self.item_data if it.item_type in types]

        # aliases = items_typed(['A', 'W', 'S'])
        aliases = items_typed(['A']) + items_typed(['W']) + items_typed(['S'])
        # generics = items_typed(['G', 'B', 'C'])
        generics = items_typed(['G']) + items_typed(['B']) + items_typed(['C'])
        others = items_typed(['L', 'I', 'T'])
        specifics = items_typed(['P'])

        self.item_data = aliases + generics + others + specifics

    def render(self) -> str:
        self.arrange()

        doc = Doc()
        rendered = set()

        for row in self.item_data:
            # .pdgitem_id could(?) refer to PDGPARTICLE.pdgitem_id which might
            # be null since isouter=True. So use [0] i.e. PDGITEM.id.
            item = row[0]

            if item in rendered:
                continue
            rendered.add(item)

            with doc.tag('tr'):
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
                text('Item type colors:')
                line('span', 'Specific, ', klass='first-legend-label specific-type')
                line('span', 'alias, ', klass='legend-label alias-type')
                line('span', 'generic, ', klass='legend-label generic-type')
                line('span', 'other', klass='legend-label other-type')
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
                        for g in groups:
                            doc.asis(g.render())

    return doc.getvalue()

def is_gauge_or_higgs(name):
    return name in ['gamma', 'g', 'graviton', 'W+', 'W-', 'Z0', 'H0', 'H']

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

def get_category(name, edition):
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
        if edition == '2023':
            return 'Pentaquarks'
        else:
            return 'Pentaquarks and tetraquarks'
    if is_unclassified(name):
        return 'Other mesons'
    return 'Error'

def main():
    os.mkdir('printouts')

    for category, groups in ItemGroup.all_categorized().items():
        raw_html = render_page(category, groups)
        html = yattag.indent(raw_html) + '\n'
        name = category.replace(' ', '_').replace('/', '_').replace("'", '_prime')
        open(f'printouts/{name}.html', 'w').write(html)


if __name__ == '__main__':
    main()
