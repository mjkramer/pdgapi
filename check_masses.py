#!/usr/bin/env python3

import pandas as pd
from sqlalchemy import select, distinct

import pdg


def main():
    api = pdg.connect()
    pdgparticle_table = api.db.tables['pdgparticle']
    pdgid_table = api.db.tables['pdgid']
    pdgdata_table = api.db.tables['pdgdata']
    query = select(distinct(pdgparticle_table.c.mcid)) \
        .where(pdgparticle_table.c.mcid != None)
    with api.engine.connect() as conn:
        mcids = [row[0] for row in conn.execute(query)]
    failures = []
    for mcid in mcids:
        try:
            p = api.get_particle_by_mcid(mcid)
            if not p.mass:
                failures.append(mcid)
        except:
            failures.append(mcid)

    pdgids = set()

    for mcid in failures:
        query = select(pdgparticle_table.c.pdgid).where(pdgparticle_table.c.mcid == mcid)
        with api.engine.connect() as conn:
            for row in conn.execute(query):
                pdgids.add(row[0])

    csv_rows = []

    for pdgid in sorted(pdgids):
        query = select(pdgid_table.c.description).where(pdgid_table.c.pdgid == pdgid)
        with api.engine.connect() as conn:
            description = conn.execute(query).scalar()

        mass_in_pdgid = False
        mass_in_pdgdata = False
        mass_non_null = False

        query = select(pdgid_table) \
            .where((pdgid_table.c.parent_pdgid == pdgid) &
                   (pdgid_table.c.data_type == 'M'))
        with api.engine.connect() as conn:
            rows_pdgid = conn.execute(query).fetchall()
        if rows_pdgid:
            mass_in_pdgid = True
            for row_pdgid in rows_pdgid:
                query = select(pdgdata_table) \
                    .where(pdgdata_table.c.pdgid == row_pdgid.pdgid)
                with api.engine.connect() as conn:
                    rows_pdgdata = conn.execute(query).fetchall()
                if rows_pdgdata:
                    mass_in_pdgdata = True
                    for row_pdgdata in rows_pdgdata:
                        if row_pdgdata.value:
                            mass_non_null = True

        csv_row = {'pdgid': pdgid,
                   'description': description,
                   'mass_in_pdgid': mass_in_pdgid,
                   'mass_in_pdgdata': mass_in_pdgdata,
                   'mass_non_null': mass_non_null}

        csv_rows.append(csv_row)

    df = pd.DataFrame(csv_rows)

    df.to_csv('missing_masses.csv', index=False)

if __name__ == '__main__':
    main()
