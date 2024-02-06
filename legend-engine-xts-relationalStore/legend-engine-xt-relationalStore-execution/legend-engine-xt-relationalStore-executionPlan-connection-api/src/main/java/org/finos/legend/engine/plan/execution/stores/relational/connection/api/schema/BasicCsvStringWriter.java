// Copyright 2024 Goldman Sachs
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

package org.finos.legend.engine.plan.execution.stores.relational.connection.api.schema;

import java.io.IOException;
import java.io.StringWriter;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.text.SimpleDateFormat;

public class BasicCsvStringWriter
{
    private static String dateFormat = "dd-MMM-yyyy";
    private static String dateTimeFormat = "dd-MMM-yyyy HH:mm:ss";
    private static String delimiter = ",";
    private static String lineBreak = "\n";

    private StringWriter stringWriter;

    public BasicCsvStringWriter(StringWriter stringWriter)
    {
        this.stringWriter = stringWriter;
    }


    public int writeResultSet(ResultSet resultSet, boolean writeHeader, boolean trim) throws IOException, SQLException
    {
        int linesWritten = 0;
        int columnCount = resultSet.getMetaData().getColumnCount();

        // Get Column names
        String [] columnNames = new String[columnCount];
        for (int i = 1; i <= columnCount; i++)
        {
            columnNames[i-1] = resultSet.getMetaData().getColumnName(i);
        }

        // write Header
        if (writeHeader)
        {
            stringWriter.write(String.join(delimiter, columnNames));
            stringWriter.write(lineBreak);
            linesWritten++;
        }

        // Write values
        while (resultSet.next())
        {
            for (int i = 1; i <= columnCount; i++)
            {
                ResultSetMetaData metaData = resultSet.getMetaData();
                String value = getValue(resultSet, metaData.getColumnType(i), i, trim);
                stringWriter.write(value);
                if (i != columnCount)
                {
                    stringWriter.write(delimiter);
                }
            }
            stringWriter.write(lineBreak);
            linesWritten++;
        }

        return linesWritten;
    }

    private String getValue(ResultSet rs, int colType, int colIndex, boolean trim) throws SQLException
    {
        String value = "";
        switch (colType)
        {
            case 91: // timestamp
                Date date = rs.getDate(colIndex);
                if (date != null)
                {
                    SimpleDateFormat df = new SimpleDateFormat(dateFormat);
                    value = df.format(date);
                }
                break;
            case 93: // date
                Object timestamp = rs.getTimestamp(colIndex);
                if (timestamp != null)
                {
                    SimpleDateFormat timeFormat = new SimpleDateFormat(dateTimeFormat);
                    value = timeFormat.format(timestamp);
                }
                break;
            default: // all other types
                Object val = rs.getObject(colIndex);
                if (val != null)
                {
                    value = val.toString();
                }
        }
        return trim ? value.trim() : value;
    }

}
