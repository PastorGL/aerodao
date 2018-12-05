package com.locomizer.aerodao;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Record;
import com.aerospike.client.command.ParticleType;
import com.aerospike.client.policy.QueryPolicy;
import com.aerospike.client.query.RecordSet;
import com.aerospike.client.query.Statement;
import io.github.pastorgl.aqlselectex.AQLSelectEx;
import io.github.pastorgl.fastdao.*;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.util.*;

public class AeroSelectDAO<E extends FastEntity> {
    static private AerospikeClient client;

    /**
     * Physical table name
     */
    private String setName;
    /**
     * {@link FastEntity} subclass
     */
    private Class<E> persistentClass;
    /**
     * Physical column names to persistent class field names mapping
     */
    private Map<String, String> revMapping = new HashMap<>();
    /**
     * Persistent class fields cache
     */
    private Map<String, Field> fields = new HashMap<>();
    /**
     * Persistent class field annotations cache
     */
    private Map<Field, Column> columns = new HashMap<>();

    private Field pkField;
    private AQLSelectEx aqlSelectEx;

    {
        Map<String, Map<String, Integer>> schema = new HashMap<>();

        persistentClass = (Class<E>) ((ParameterizedType) getClass()
                .getGenericSuperclass()).getActualTypeArguments()[0];

        if (persistentClass.isAnnotationPresent(Table.class)) {
            setName = persistentClass.getAnnotation(Table.class).value();
        } else {
            setName = persistentClass.getSimpleName();
        }

        Map<String, Integer> setSchema = new HashMap<>();

        for (Field field : persistentClass.getDeclaredFields()) {
            if ((field.getModifiers() & Modifier.STATIC) == 0) {
                field.setAccessible(true);
                String fieldName = field.getName();

                fields.put(fieldName, field);

                String columnName;
                if (field.isAnnotationPresent(Column.class)) {
                    Column column = field.getAnnotation(Column.class);
                    columnName = column.value();
                    revMapping.put(columnName, fieldName);

                    columns.put(field, column);

                    int particleType = ParticleType.STRING;

                    if (field.getType().isAssignableFrom(Double.class)) {
                        particleType = ParticleType.DOUBLE;
                    } else if (field.getType().isAssignableFrom(Number.class)) {
                        particleType = ParticleType.INTEGER;
                    } else if (field.getType().isAssignableFrom(Map.class)) {
                        particleType = ParticleType.MAP;
                    } else if (field.getType().isAssignableFrom(List.class)) {
                        particleType = ParticleType.LIST;
                    }

                    setSchema.put(columnName, particleType);
                }

                if (field.isAnnotationPresent(PK.class)) {
                    pkField = field;
                }
            }
        }

        schema.put(setName, setSchema);

        aqlSelectEx = AQLSelectEx.forSchema(schema);
    }

    static public void setClient(AerospikeClient client) {
        AeroSelectDAO.client = client;
    }

    /**
     * Call SELECT that returns a lizt of &lt;E&gt; instances
     *
     * @param query any SQL Query whose result is a list of &lt;E&gt;, optionally with ? for replaceable parameters.
     *              Use backslash to escape question marks
     * @param args  objects, whose values will be used as source of replaceable parameters. If object is an array or
     *              {@link List}, it'll be unfolded
     * @return list of &lt;E&gt;
     */
    protected List<E> select(String query, Object... args) {
        try {
            if (args.length != 0) {
                List<Object> expl = new ArrayList<>(args.length);

                StringBuilder sb = new StringBuilder();

                int r = 0;
                for (Object a : args) {
                    int q;
                    boolean found = false;
                    do {
                        q = query.indexOf('?', r);
                        if (q < 0) {
                            throw new IllegalArgumentException("supplied query and replaceable arguments don't match");
                        }
                        if ((q > 0) && (query.charAt(q - 1) == '\\')) {
                            r = q + 1;
                        } else {
                            found = true;
                        }
                    } while (!found);
                    sb.append(query.substring(r, q));
                    r = q + 1;

                    if (a instanceof Object[]) {
                        a = Arrays.asList(a);
                    }
                    if (a instanceof List) {
                        List<Object> aa = (List<Object>) a;
                        int s = aa.size();
                        sb.append('(');
                        for (int i = 0; i < s; i++) {
                            expl.add(aa.get(i));
                            if (i > 0) {
                                sb.append(',');
                            }
                            sb.append('?');
                        }
                        sb.append(')');
                    } else {
                        expl.add(a);
                        sb.append('?');
                    }
                }

                sb.append(query.substring(r));

                query = sb.toString();
                args = expl.toArray();
            }

            for (Object a : args) {
                query = setObject(query, a);
            }

            List<E> lst = new ArrayList<>();

            Statement statement = aqlSelectEx.fromString(query);

            boolean pk = (pkField != null);
            QueryPolicy qp = null;
            if (pk) {
                qp = new QueryPolicy();
                qp.sendKey = true;
            }

            RecordSet rs = client.query(qp, statement);

            while (rs.next()) {
                E e = persistentClass.newInstance();

                Record rec = rs.getRecord();

                for (String colName : rec.bins.keySet()) {
                    Field field = fields.get(getRevMapping(colName));
                    Class<?> type = field.getType();

                    if (type.isEnum()) {
                        field.set(e, Enum.valueOf((Class<Enum>) type, rec.getString(colName)));
                    } else if (type.isAssignableFrom(Boolean.class)) {
                        convertFromRetrieve(field, e, rec.getBoolean(colName));
                    } else if (type.isAssignableFrom(List.class)) {
                        convertFromRetrieve(field, e, rec.getList(colName));
                    } else if (type.isAssignableFrom(Map.class)) {
                        convertFromRetrieve(field, e, rec.getMap(colName));
                    } else {
                        convertFromRetrieve(field, e, rec.getValue(colName));
                    }
                }

                if (pk) {
                    pkField.set(e, rs.getKey().userKey.getObject());
                }

                lst.add(e);
            }

            return lst;
        } catch (Exception e) {
            throw new FastDAOException("select", e);
        }
    }

    private String setObject(String query, Object a) {
        if (a instanceof FastEntity) {
            query = query.replaceFirst("\\?", ((FastEntity) a).getId().toString());
            return query;
        }

        if (a instanceof java.util.Date) {
            query = query.replaceFirst("\\?", new java.sql.Date(((java.util.Date) a).getTime()).toString());
            return query;
        }

        if (a instanceof Enum) {
            query = query.replaceFirst("\\?", ((Enum<?>) a).name());
            return query;
        }

        query = query.replaceFirst("\\?", a.toString());

        return query;
    }

    private String getRevMapping(String columnName) {
        if (revMapping.containsKey(columnName)) {
            return revMapping.get(columnName);
        }

        return columnName;
    }

    private void convertFromRetrieve(Field field, Object object, Object dbValue) throws Exception {
        Object value = dbValue;
        if (columns.containsKey(field)) {
            value = columns.get(field).retrieve().newInstance().retrieve(dbValue);
        }

        field.set(object, value);
    }
}
