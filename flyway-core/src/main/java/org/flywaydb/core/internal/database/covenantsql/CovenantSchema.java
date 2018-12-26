/*
 * Copyright 2010-2018 Boxfuse GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.flywaydb.core.internal.database.covenantsql;

import org.flywaydb.core.api.logging.Log;
import org.flywaydb.core.api.logging.LogFactory;
import org.flywaydb.core.internal.database.base.Schema;
import org.flywaydb.core.internal.database.base.Table;
import org.flywaydb.core.internal.jdbc.JdbcTemplate;

import java.sql.SQLException;
import java.util.List;

public class CovenantSchema extends Schema<CovenantDatabase> {
    private static final Log LOG = LogFactory.getLog(CovenantSchema.class);

    public CovenantSchema(JdbcTemplate jdbcTemplate, CovenantDatabase database, String name) {
        super(jdbcTemplate, database, name);
    }

    @Override
    protected boolean doExists() throws SQLException {
        try {
            doAllTables();
            return true;
        } catch (SQLException e) {
            return false;
        }
    }

    @Override
    protected boolean doEmpty() throws SQLException {
        return allTables().length == 0;
    }

    @Override
    protected void doCreate() throws SQLException {
        LOG.info("CovenantSQL does not support creating schemas. Schema not created: " + name);
    }

    @Override
    protected void doDrop() throws SQLException {
        LOG.info("CovenantSQL does not support dropping schemas. Schema not dropped: " + name);
    }

    @Override
    protected void doClean() throws SQLException {
        for (Table table : allTables()) {
            table.drop();
        }
    }

    @Override
    protected Table[] doAllTables() throws SQLException {
        List<String> tableNames = jdbcTemplate.queryForStringList("SHOW TABLES");

        Table[] tables = new Table[tableNames.size()];
        for (int i = 0; i < tableNames.size(); i++) {
            tables[i] = new CovenantTable(jdbcTemplate, database, this, tableNames.get(i));
        }

        return tables;
    }

    @Override
    public Table getTable(String tableName) {
        return new CovenantTable(jdbcTemplate, database, this, tableName);
    }
}
