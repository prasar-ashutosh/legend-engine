// Copyright 2022 Goldman Sachs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.finos.legend.engine.language.pure.dsl.persistence.mapper;

import com.gs.alloy.components.logicalplan.datasets.Dataset;
import com.gs.alloy.components.logicalplan.datasets.DatasetDefinition;
import com.gs.alloy.components.logicalplan.datasets.JsonExternalDatasetReference;
import com.gs.alloy.components.logicalplan.datasets.SchemaDefinition;
import org.eclipse.collections.api.RichIterable;
import org.finos.legend.pure.generated.Root_meta_pure_persistence_metamodel_Persistence;
import org.finos.legend.pure.generated.Root_meta_pure_persistence_metamodel_persister_sink_Sink;
import org.finos.legend.pure.generated.Root_meta_pure_persistence_metamodel_persister_targetshape_TargetShape;
import org.finos.legend.pure.generated.Root_meta_pure_persistence_metamodel_persister_BatchPersister;
import org.finos.legend.pure.generated.Root_meta_pure_persistence_metamodel_persister_sink_RelationalSink;
import org.finos.legend.pure.generated.Root_meta_pure_persistence_metamodel_persister_targetshape_FlatTarget;
import org.finos.legend.pure.m3.coreinstance.meta.relational.metamodel.Database;
import org.finos.legend.pure.m3.coreinstance.meta.relational.metamodel.SchemaAccessor;
import org.finos.legend.pure.m3.coreinstance.meta.relational.metamodel.relation.Table;
import org.finos.legend.pure.generated.Root_meta_pure_persistence_metamodel_persister_Persister;


public class DatasetMapper
{

    public static Dataset getTargetDataset(Root_meta_pure_persistence_metamodel_Persistence persistence) throws Exception
    {
        // persister
        Root_meta_pure_persistence_metamodel_persister_Persister persister = persistence._persister();
        if (!(persister instanceof Root_meta_pure_persistence_metamodel_persister_BatchPersister))
        {
            throw new Exception("write-component-test only supports BatchPersister");
        }
        Root_meta_pure_persistence_metamodel_persister_BatchPersister batchPersister = (Root_meta_pure_persistence_metamodel_persister_BatchPersister) persister;

        // sink
        Root_meta_pure_persistence_metamodel_persister_sink_Sink sink = persister._sink();
        if (!(sink instanceof Root_meta_pure_persistence_metamodel_persister_sink_RelationalSink))
        {
            throw new Exception("write-component-test only supports RelationalSink");
        }
        Root_meta_pure_persistence_metamodel_persister_sink_RelationalSink relationalSink = (Root_meta_pure_persistence_metamodel_persister_sink_RelationalSink) sink;
        Database database = relationalSink._database();

        // target shape
        Root_meta_pure_persistence_metamodel_persister_targetshape_TargetShape targetShape = batchPersister._targetShape();
        if (!(targetShape instanceof Root_meta_pure_persistence_metamodel_persister_targetshape_FlatTarget))
        {
            throw new Exception("write-component-test only supports FlatTarget");
        }
        Root_meta_pure_persistence_metamodel_persister_targetshape_FlatTarget flatTarget = (Root_meta_pure_persistence_metamodel_persister_targetshape_FlatTarget) targetShape;

        RichIterable<? extends Table> tables = database._schemas().flatCollect(SchemaAccessor::_tables);
        Table table = tables.detect(t -> t._name().equals(flatTarget._targetName()));
        String dbName = database._name();
        String tableName = table._name();
        String schemaName = table._schema()._name();

        System.out.println(String.format("DB: %s, Table: %s, Schema: %s", dbName, tableName, schemaName));

        // TODO derive the schema definition of main table from table._columns()
        SchemaDefinition definition = SchemaDefinition.builder().build();

        DatasetDefinition datasetDefinition = DatasetDefinition.builder()
                .database(dbName)
                .name(tableName)
                .group(schemaName)
                .alias(tableName)
                .schemaDefinition(null)
                .build();
        return datasetDefinition;
    }

    public static Dataset getStagingDataset(Dataset targetDataset, String jsonData) throws Exception
    {
        // TODO derive the schema definiton of stagingTable from targetDataset and jsonData
        JsonExternalDatasetReference stagingDataset = new JsonExternalDatasetReference(jsonData, null);
        return stagingDataset;
    }

}
