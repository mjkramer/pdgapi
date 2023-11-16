"""
Test cases for PdgParticle.
"""
from __future__ import print_function

import unittest

import pdg
from pdg.errors import PdgAmbiguousValueError, PdgNoDataError


class TestData(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.api = pdg.connect(pedantic=False)

    def test_all_particle_data(self):
        n_errors = 0
        for p in self.api.get_particles():
            try:
                p._get_particle_data()
            except Exception as e:
                n_errors += 1
                print(e)
        self.assertEqual(n_errors, 0)

    def test_name(self):
        self.assertEqual(self.api.get_particle_by_name('p').mcid, 2212)
        self.assertEqual(self.api.get_particle_by_name('pbar').mcid, -2212)

    def test_mcid(self):
        self.assertEqual(self.api.get_particle_by_mcid(5).name, 'b')
        self.assertEqual(self.api.get_particle_by_mcid(-5).name, 'bbar')
        self.assertEqual(self.api.get_particle_by_mcid(5).mcid, 5)
        self.assertEqual(self.api.get_particle_by_mcid(-5).mcid, -5)
        self.assertEqual(self.api.get_particle_by_mcid(11).name, 'e-')
        self.assertEqual(self.api.get_particle_by_mcid(-11).name, 'e+')
        self.assertEqual(self.api.get_particle_by_mcid(39).name, 'graviton')
        self.assertEqual(self.api.get_particle_by_mcid(100211).name, 'pi(1300)+')
        self.assertEqual(self.api.get_particle_by_mcid(-30323).name, 'K^*(1680)-')
        self.assertEqual(self.api.get_particle_by_mcid(-30323).mcid, -30323)

    def test_quantum_P(self):
        self.assertEqual(self.api.get_particle_by_name('u').quantum_P, '+')
        self.assertEqual(self.api.get_particle_by_name('ubar').quantum_P, '-')
        self.assertEqual(self.api.get_particle_by_name('t').quantum_P, '+')
        self.assertEqual(self.api.get_particle_by_name('tbar').quantum_P, '-')
        self.assertEqual(self.api.get_particle_by_name('p').quantum_P, '+')
        self.assertEqual(self.api.get_particle_by_name('pbar').quantum_P, '-')
        self.assertEqual(self.api.get_particle_by_name('n').quantum_P, '+')
        self.assertEqual(self.api.get_particle_by_name('nbar').quantum_P, '-')
        self.assertEqual(self.api.get_particle_by_mcid(3122).quantum_P, '+')
        self.assertEqual(self.api.get_particle_by_mcid(-3122).quantum_P, '-')

    def test_mass_2022(self):
        if '2022' not in self.api.editions:
            return
        masses = list(self.api.get('q007/2022').masses())
        self.assertEqual(masses[0].pdgid, 'Q007TP/2022')
        self.assertEqual(round(masses[0].best_summary().value,9), 172.687433378)
        self.assertEqual(masses[1].pdgid, 'Q007TP2/2022')
        self.assertEqual(round(masses[1].best_summary().value,9), 162.500284698)
        self.assertEqual(masses[2].pdgid, 'Q007TP4/2022')
        self.assertEqual(round(masses[2].best_summary().value,9), 172.463424407)
        self.assertEqual(round(self.api.get('s008/2022').mass,9), 0.139570391)
        self.assertEqual(round(self.api.get('s008/2022').mass_error,16), 1.820071604e-07)

    def test_flags(self):
        self.assertEqual(self.api.get('S008').is_boson, False)
        self.assertEqual(self.api.get('S008').is_quark, False)
        self.assertEqual(self.api.get('S008').is_lepton, False)
        self.assertEqual(self.api.get('S008').is_meson, True)
        self.assertEqual(self.api.get('S008').is_baryon, False)
        self.assertEqual(self.api.get('q007').is_quark, True)
        self.assertEqual(self.api.get('s000').is_boson, True)
        self.assertEqual(self.api.get('S003').is_lepton, True)
        self.assertEqual(self.api.get('S041').is_meson, True)
        self.assertEqual(self.api.get('S016').is_baryon, True)

    def test_properties(self):
        self.assertEqual(len(list(self.api.get('S017').properties('M'))), 2)

    def test_ambiguous_defaults(self):
        self.assertEqual(round(self.api.get('Q007').mass, 1), 172.7)
        self.assertEqual(self.api.get('S013D').best_summary().comment, 'Assuming CPT')

    def test_best_widths_and_lifetimes(self):
        saved_pedantic = self.api.pedantic

        pi0 = self.api.get_particle_by_name('pi0')
        self.assertTrue(pi0.has_lifetime_entry)
        self.assertFalse(pi0.has_width_entry)
        self.assertEqual(pi0.lifetime, 8.42551220525037e-17)
        self.assertEqual(pi0.lifetime_error, 1.34474888523086e-18)
        self.api.pedantic = True
        self.assertRaises(PdgNoDataError, lambda: pi0.width)
        self.assertRaises(PdgNoDataError, lambda: pi0.width_error)
        self.api.pedantic = False
        self.assertEqual(pi0.width, 7.81198797136442e-09)
        self.assertEqual(pi0.width_error, 1.2468277132615016e-10)

        W = self.api.get_particle_by_name('W')
        self.assertTrue(W.has_width_entry)
        self.assertFalse(W.has_lifetime_entry)
        self.assertEqual(W.width, 2.085)
        self.assertEqual(W.width_error, 0.042)
        self.api.pedantic = True
        self.assertRaises(PdgNoDataError, lambda: W.lifetime)
        self.assertRaises(PdgNoDataError, lambda: W.lifetime_error)
        self.api.pedantic = False
        self.assertEqual(W.lifetime, 3.156834532374101e-25)
        self.assertEqual(W.lifetime_error, 6.359091144350707e-27)

        self.api.pedantic = saved_pedantic

    def test_kstar_892(self):
        saved_pedantic = self.api.pedantic

        self.api.pedantic = False
        p = self.api.get('M018')
        self.assertTrue(p.is_generic)
        self.assertEqual(len(list(p.masses())), 4)
        self.assertEqual(len(list(p.widths())), 3)
        self.assertEqual(list(p.lifetimes()), [])
        self.assertRaises(PdgAmbiguousValueError, lambda: p.mass)
        self.assertRaises(PdgAmbiguousValueError, lambda: p.width)
        self.assertRaises(PdgAmbiguousValueError, lambda: p.lifetime)
        self.assertEqual(p.charge, None)

        self.api.pedantic = True
        p = self.api.get('M018')
        self.assertRaises(PdgAmbiguousValueError, lambda: p.is_generic)
        self.assertRaises(PdgAmbiguousValueError, lambda: list(p.masses()))
        self.assertRaises(PdgAmbiguousValueError, lambda: list(p.widths()))
        self.assertEqual(list(p.lifetimes()), [])
        self.assertRaises(PdgAmbiguousValueError, lambda: p.mass)
        self.assertRaises(PdgAmbiguousValueError, lambda: p.width)
        self.assertRaises(PdgAmbiguousValueError, lambda: p.lifetime)
        self.assertRaises(PdgAmbiguousValueError, lambda: p.charge)

        for self.api.pedantic in [True, False]:
            p = self.api.get_particle_by_mcid(323)
            self.assertFalse(p.is_generic)
            self.assertEqual(len(list(p.masses())), 3)
            self.assertEqual(len(list(p.widths())), 2)
            self.assertEqual(len(list(p.lifetimes())), 0)
            self.assertEqual(p.charge, 1.0)
        self.api.pedantic = False
        p = self.api.get_particle_by_mcid(323)
        self.assertEqual(round(p.mass, 3), 0.892)
        self.assertEqual(round(p.width, 4), 0.0514)
        self.assertEqual(round(p.lifetime * 1e23, 2), 1.28)
        self.api.pedantic = True
        p = self.api.get_particle_by_mcid(323)
        self.assertRaises(PdgAmbiguousValueError, lambda: p.mass)
        self.assertRaises(PdgAmbiguousValueError, lambda: p.width)
        self.assertRaises(PdgNoDataError, lambda: p.lifetime)

        for self.api.pedantic in [True, False]:
            p = self.api.get_particle_by_mcid(-323)
            self.assertFalse(p.is_generic)
            self.assertEqual(len(list(p.masses())), 3)
            self.assertEqual(len(list(p.widths())), 2)
            self.assertEqual(len(list(p.lifetimes())), 0)
            self.assertEqual(p.charge, -1.0)
        self.api.pedantic = False
        p = self.api.get_particle_by_mcid(-323)
        self.assertEqual(round(p.mass, 3), 0.892)
        self.assertEqual(round(p.width, 4), 0.0514)
        self.assertEqual(round(p.lifetime * 1e23, 2), 1.28)
        self.api.pedantic = True
        p = self.api.get_particle_by_mcid(-323)
        self.assertRaises(PdgAmbiguousValueError, lambda: p.mass)
        self.assertRaises(PdgAmbiguousValueError, lambda: p.width)
        self.assertRaises(PdgNoDataError, lambda: p.lifetime)

        for self.api.pedantic in [True, False]:
            p = self.api.get_particle_by_mcid(313)
            self.assertFalse(p.is_generic)
            self.assertEqual(len(list(p.masses())), 2)
            self.assertEqual(len(list(p.widths())), 1)
            self.assertEqual(len(list(p.lifetimes())), 0)
            self.assertEqual(p.charge, 0.0)
        self.api.pedantic = False
        p = self.api.get_particle_by_mcid(313)
        self.assertEqual(round(p.mass, 3), 0.896)
        self.assertEqual(round(p.width, 4), 0.0473)
        self.assertEqual(round(p.lifetime * 1e23, 2), 1.39)
        self.api.pedantic = True
        p = self.api.get_particle_by_mcid(313)
        self.assertRaises(PdgAmbiguousValueError, lambda: p.mass)
        self.assertEqual(round(p.width, 4), 0.0473)
        self.assertRaises(PdgNoDataError, lambda: p.lifetime)

        self.api.pedantic = saved_pedantic
