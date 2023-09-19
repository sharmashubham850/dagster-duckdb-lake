from jaffle.assets import population, continent_population
from jaffle.duckpond import DuckDB

def test_assets():
    p = population()
    cp = continent_population(p)

    assert (cp.sql.lower() == 'select continent, avg(pop_change) as avg_pop_change from $population group by 1 order by 2 desc')
    assert 'population' in cp.bindings

    df = DuckDB().query(cp)
    top = df.loc[0]
    print(df)
    assert top['continent'] == 'Africa'
    assert round(top['avg_pop_change']) == 2