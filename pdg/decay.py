"""
Classes supporting decays and branching fractions/ratios.
"""

from dataclasses import dataclass
from typing import Optional, Iterable

from sqlalchemy import bindparam, select, RowMapping

from pdg.api import PdgApi
from pdg.data import PdgProperty
from pdg.errors import PdgInvalidPdgIdError, PdgNoDataError
from pdg.particle import PdgParticle


class PdgItem:
    def __init__(self, api: PdgApi, pdgitem_id: int):
        self.api = api
        self.pdgitem_id = pdgitem_id
        self.cache = {}

    def _get_pdgitem(self) -> RowMapping:
        if 'pdgitem' not in self.cache:
            pdgitem_table = self.api.db.tables['pdgitem']
            query = select(pdgitem_table).where(pdgitem_table.c.pdgitem_id == bindparam('pdgitem_id'))
            with self.api.engine.connect() as conn:
                result = conn.execute(query, {'pdgitem_id': self.pdgitem_id}).fetchone()
                if result is None:
                    raise PdgNoDataError(f'No PDGITEM entry for {self.pdgitem_id}')
                self.cache['pdgitem'] = result._mapping
        return self.cache['pdgitem']

    @property
    def has_particle(self) -> bool:
        if 'has_particle' not in self.cache:
            pdgparticle_table = self.api.db.tables['pdgparticle']
            query = select(pdgparticle_table).where(pdgparticle_table.c.pdgitem_id == bindparam('pdgitem_id'))
            with self.api.engine.connect() as conn:
                result = conn.execute(query, {'pdgitem_id': self.pdgitem_id}).fetchone()
                if result:
                    self.cache['pdgparticle'] = result._mapping
                    self.cache['has_particle'] = True
                else:
                    self.cache['has_particle'] = False
        return self.cache['has_particle']

    @property
    def particle(self) -> PdgParticle:
        if not self.has_particle:
            raise PdgNoDataError(f'No PDGPARTICLE for PDGITEM {self.pdgitem_id}')
        p = self.cache['pdgparticle']
        return PdgParticle(self.api, p['pdgid'], set_mcid=p['mcid'])


@dataclass
class PdgDecayProduct:
    item: PdgItem
    multiplier: int
    subdecay: Optional['PdgBranchingFraction']


class PdgBranchingFraction(PdgProperty):
    def _get_decay(self) -> list[dict]:
        if 'pdgdecay' not in self.cache:
            pdgdecay_table = self.api.db.tables['pdgdecay']
            query = select(pdgdecay_table).where(pdgdecay_table.c.pdgid == bindparam('pdgid'))
            with self.api.engine.connect() as conn:
                try:
                    result = conn.execute(query, {'pdgid': self.baseid}).fetchall()
                    self.cache['pdgdecay'] = [row._mapping for row in result]
                except AttributeError:
                    raise PdgInvalidPdgIdError(f'No PDGDECAY entry for {self.pdgid}')
        return self.cache['pdgdecay']

    def _get_particle(self, pdgitem_id: int) -> PdgParticle:
        particle_table = self.api.db.tables['pdgparticle']
        query = select(particle_table).where(particle_table.c.pdgitem_id == bindparam('pdgitem_id'))
        with self.api.engine.connect() as conn:
            try:
                row = conn.execute(query, {'pdgitem_id': pdgitem_id}).fetchone()._mapping
                return PdgParticle(self.api, row['pdgid'], set_mcid=row['mcid'])
            except AttributeError:
                raise PdgNoDataError(f'No PDGPARTICLE entry with pdgitem_id of {pdgitem_id}')

    def _get_targets(self, pdgitem_id: int) -> Iterable[int]:
        pdgitem_map_table = self.api.db.tables['pdgitem_map']
        query = select(pdgitem_map_table).where(pdgitem_map_table.c.pdgitem_id == bindparam('pdgitem_id'))
        with self.api.engine.connect() as conn:
            rows = conn.execute(query, {'pdgitem_id': pdgitem_id}).fetchall()
            for row in rows:
                yield row.target_id

    def _get_all_particles(self, pdgitem_id: int) -> Iterable[PdgParticle]:
        # XXX no good?
        targets = list(self._get_targets(pdgitem_id))
        if len(targets) == 0:
            yield self._get_particle(pdgitem_id)
            return
        for tgt_pdgitem_id in targets:
            yield from self._get_all_particles(tgt_pdgitem_id)

    def _get_all_items(self, pdgitem_id: int) -> Iterable[PdgItem]:
        targets = list(self._get_targets(pdgitem_id))
        if len(targets) == 0:
            yield PdgItem(self.api, pdgitem_id)
            return
        for target_pdgitem_id in targets:
            yield from self._get_all_items(target_pdgitem_id)


    @property
    def products(self):
        for row in self._get_decay():
            if not row['is_outgoing']:
                continue

            yield PdgDecayProduct(
                # particle=self._get_pdgparticle(row['pdgitem_id']),
                item=PdgItem(self.api, row['pdgitem_id']),
                multiplier=row['multiplier'],
                subdecay=(PdgBranchingFraction(self.api, row['subdecay'])
                          if row['subdecay_id'] else None))

    @property
    def mode_number(self):
        """Mode number of this decay.

        Note that the decay mode number may change from one edition of the Review of Particle Physics
        to the next one."""
        return self._get_pdgid()['mode_number']

    @property
    def is_subdecay(self):
        """True if this is a subdecay ("indented") decay mode."""
        data_type_code = self.data_type
        if len(data_type_code) < 4:
            return False
        else:
            return data_type_code[0:3] == 'BFX' or data_type_code[0:3] == 'BFI'

    @property
    def subdecay_level(self):
        """Return indentation level of a decay mode."""
        if self.is_subdecay:
            return int(self.data_type[3])
        else:
            return 0
