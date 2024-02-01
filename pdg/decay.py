"""
Classes supporting decays and branching fractions/ratios.
"""

from dataclasses import dataclass
from typing import Optional

from sqlalchemy import bindparam, select

from pdg.data import PdgProperty
from pdg.errors import PdgInvalidPdgIdError, PdgNoDataError
from pdg.particle import PdgParticle


@dataclass
class PdgDecayProduct:
    particle: PdgParticle
    multiplier: int
    subdecay: Optional['PdgBranchingFraction']


class PdgBranchingFraction(PdgProperty):
    def _get_pdgdecay(self):
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

    def _get_particle(self, pdgitem_id):
        particle_table = self.api.db.tables['pdgparticle']
        query = select(particle_table).where(particle_table.c.pdgitem_id == bindparam('pdgitem_id'))
        with self.api.engine.connect() as conn:
            try:
                row = conn.execute(query, {'pdgitem_id': pdgitem_id}).fetchone()._mapping
                return PdgParticle(self.api, row['pdgid'], set_mcid=row['mcid'])
            except AttributeError:
                raise PdgNoDataError(f'No PDGPARTICLE entry with pdgitem_id of {pdgitem_id}')

    @property
    def products(self):
        for row in self._get_pdgdecay():
            if not row['is_outgoing']:
                continue

            yield PdgDecayProduct(
                particle=self._get_particle(row['pdgitem_id']),
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
