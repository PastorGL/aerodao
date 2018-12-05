package io.github.pastorgl.aerodao;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.policy.WritePolicy;
import com.locomizer.aerodao.AeroSelectDAO;
import io.github.pastorgl.fastdao.*;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.junit.Assert.assertTrue;

public class AeroDAOTest {
    private static final AerospikeClient client = new AerospikeClient(null, System.getProperty("aerospike.host", "localhost"), new Integer(System.getProperty("aerospike.port", "3000")));

    @BeforeClass
    public static void setup() {
        AeroSelectDAO.setClient(client);
    }

    @Test
    public void operationsTest() {
        try {

            WritePolicy wp = new WritePolicy();
            wp.sendKey = true;

            client.add(wp, new Key("test", "entity", "PK1"), new Bin[]{
                    new Bin("varchar", "aaaaaa"),
                    new Bin("enum", "B"),
                    new Bin("bool", false),
                    new Bin("list", Arrays.asList(3, 4, 5))
            });

            TestDAO dao = new TestDAO();

            TestEntity _one = dao.select("SELECT * FROM test.entity").get(0);

            TestEntity one = new TestEntity();
            one.setBool(false);
            one.setEnum(TestEnum.B);
            one.setId("PK1");
            one.setList(Arrays.asList(3, 4, 5));
            one.setVarchar("aaaaaa");

            assertTrue(one.equals(_one));
        } finally {
            client.truncate(null, "test", "entity", null);
        }
    }

    @Table("test.entity")
    public static class TestEntity extends FastEntity {
        @PK
        private String id;

        @Column("varchar")
        private String varchar;

        @Column("enum")
        private TestEnum _enum;

        @Column("bool")
        private Boolean bool;

        @Column(value = "list", retrieve = TestConverter.class)
        private List<Integer> list;

        @Override
        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        public List<Integer> getList() {
            return list;
        }

        public void setList(List<Integer> list) {
            this.list = list;
        }

        public String getVarchar() {
            return varchar;
        }

        public void setVarchar(String varchar) {
            this.varchar = varchar;
        }

        public TestEnum getEnum() {
            return _enum;
        }

        public void setEnum(TestEnum _enum) {
            this._enum = _enum;
        }

        public Boolean getBool() {
            return bool;
        }

        public void setBool(Boolean bool) {
            this.bool = bool;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof TestEntity)) return false;
            TestEntity that = (TestEntity) o;
            return Objects.equals(id, that.id) &&
                    Objects.equals(varchar, that.varchar) &&
                    _enum == that._enum &&
                    Objects.equals(bool, that.bool) &&
                    list.equals(that.list);
        }

        @Override
        public int hashCode() {
            return Objects.hash(id, varchar, _enum, bool, list);
        }
    }

    public static class TestDAO extends AeroSelectDAO<TestEntity> {
        @Override
        protected List<TestEntity> select(String query, Object... args) {
            return super.select(query, args);
        }
    }

    public enum TestEnum {
        A,
        B,
        C;
    }

    public static class TestConverter implements RetrieveConverter {
        @Override
        public Object retrieve(Object dbValue) {
            List<Long> retrieved = (List<Long>)dbValue;
            return retrieved.stream().map(l -> l.intValue()).collect(Collectors.toList());
        }
    }
}
