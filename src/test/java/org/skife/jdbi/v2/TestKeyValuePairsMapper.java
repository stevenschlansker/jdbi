package org.skife.jdbi.v2;

import static org.junit.Assert.assertEquals;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

import org.h2.jdbcx.JdbcDataSource;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.skife.jdbi.v2.sqlobject.SqlQuery;
import org.skife.jdbi.v2.sqlobject.customizers.Mapper;
import org.skife.jdbi.v2.tweak.ResultSetMapper;

public class TestKeyValuePairsMapper {

    private static final String SELECT = "SELECT k, v FROM kv";
    private static final Map<String, String> TEST_MAP = ImmutableMap.of(
            "speed", "88mph",
            "power", "1.21 gigawatts",
            "make", "DeLorean",
            "destination", "1985"
        );

    private Handle h;

    @Before
    public void fillData() throws Exception {
        JdbcDataSource ds = new JdbcDataSource();
        ds.setURL("jdbc:h2:mem:" + UUID.randomUUID());
        h = new DBI(ds).open();
        h.execute("CREATE TABLE kv(k varchar, v varchar)");
        for (Entry<String, String> e : TEST_MAP.entrySet()) {
            h.execute("INSERT INTO kv VALUES(?,?)", e.getKey(), e.getValue());
        }
    }

    @After
    public void closeHandle() throws Exception {
        h.close();
    }

    @Test
    public void testSqlObject() throws Exception {
        Map<String, String> attrs = h.attach(BiffDao.class).getAttrs();
        assertEquals(TEST_MAP, attrs);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testFluent() throws Exception {
        Map<String, String> attrs = h
                .createQuery(SELECT)
                .map(new EntryMapper())
                .collect(new EntryCollector());
        assertEquals(TEST_MAP, attrs);
    }

    public interface BiffDao {
        @SqlQuery(SELECT)
        @Mapper(EntryMapper.class)
        @Collector(EntryCollector.class)
        Map<String, String> getAttrs();
    }

    class EntryMapper implements ResultSetMapper<Entry<String, String>> {

        @Override
        public Entry<String, String> map(int index, ResultSet r, StatementContext ctx) throws SQLException {
            return Maps.immutableEntry(r.getString("k"), r.getString("v"));
        }
    }

    class EntryCollector implements ContainerBuilder<Map<String, String>> {

        private final Map<String, String> result = new HashMap<String, String>();

        @Override
        public ContainerBuilder<Map<String, String>> add(Object it) {
            Entry<String, String> e = (Entry<String, String>) it;
            result.put(e.getKey(), e.getValue());
            return this;
        }

        @Override
        public Map<String, String> build() {
            return result;
        }

    }
}
