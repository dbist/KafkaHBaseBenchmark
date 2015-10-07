package com.hortonworks;

import org.apache.commons.lang3.ArrayUtils;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Created by christoph on 8/24/15.
 */
public class MappingConfig implements Serializable {
    
    public Field[] fields = new Field[0];
    public Field[] partitionFields = new Field[0];

    public String[] dateFormats;


    public String[] getCombinedFieldNames() {
        String[] r = new String[fields.length + partitionFields.length];
        List<String> l = new ArrayList<String>();
        addFieldNames(fields, l);
        addFieldNames(partitionFields, l);
        return l.toArray(r);
    }

    public Field[] getCombinedFields() {
        return ArrayUtils.addAll(fields, partitionFields);
    }

    public String[] getFieldNames() {
        String[] r = new String[fields.length];
        List<String> l = new ArrayList<String>();
        addFieldNames(fields, l);
        return l.toArray(r);
    }

    public String[] getPartitionFieldNames() {
        String[] r = new String[partitionFields.length];
        List<String> l = new ArrayList<String>();
        addFieldNames(partitionFields, l);
        return l.toArray(r);
//        return Arrays.asList(fields)
//                .stream().map(f -> f.name)
//                .collect((Collectors.toList()))
//                .toArray(new String[fields.length]);
    }

    private void addFieldNames(Field[] fields, List<String> l) {
        for(Field f : fields){
            l.add(f.name);
        }
    }


}
