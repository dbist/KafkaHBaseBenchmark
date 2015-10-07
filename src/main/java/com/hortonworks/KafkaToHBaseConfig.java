package com.hortonworks;

import com.google.gson.Gson;

import java.io.Serializable;

/**
 * Created by christoph on 8/24/15.
 */
public class KafkaToHBaseConfig implements Serializable{

    public MappingConfig mappingConfig = new MappingConfig();

    public String id = "";

    @Override
    public String toString(){
        return new Gson().toJson(this);
    }

    public static KafkaToHBaseConfig parse(String config){
        return new Gson().fromJson(config, KafkaToHBaseConfig.class);
    }

    public String getId() {
        return id;
    }
    public void setId(String id)
    {
        this.id=id;
    }

}
