package com.altamiracorp.lumify.wikipedia.mapreduce;

import org.apache.accumulo.core.data.Mutation;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MutationOrElasticSearchIndexWritable implements Writable {
    public static final int TYPE_ES = 1;
    public static final int TYPE_MUTATION = 2;

    private int type;
    private String id;
    private String json;
    private Mutation mutation;

    public MutationOrElasticSearchIndexWritable() {

    }

    public MutationOrElasticSearchIndexWritable(String id, String json) {
        this.type = TYPE_ES;
        this.id = id;
        this.json = json;
    }

    public MutationOrElasticSearchIndexWritable(Mutation mutation) {
        this.type = TYPE_MUTATION;
        this.mutation = mutation;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeByte(type);
        if (this.type == TYPE_ES) {
            byte[] idBytes = this.id.getBytes();
            byte[] jsonBytes = this.json.getBytes();
            out.writeInt(idBytes.length);
            out.writeInt(jsonBytes.length);
            out.write(idBytes);
            out.write(jsonBytes);
        } else if (this.type == TYPE_MUTATION) {
            this.mutation.write(out);
        } else {
            throw new IOException("Invalid type: " + this.type);
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.type = in.readByte();
        if (this.type == TYPE_ES) {
            int idBytesLength = in.readInt();
            int jsonBytesLength = in.readInt();
            byte[] buffer = new byte[Math.max(idBytesLength, jsonBytesLength)];
            in.readFully(buffer, 0, idBytesLength);
            this.id = new String(buffer, 0, idBytesLength);
            in.readFully(buffer, 0, jsonBytesLength);
            this.json = new String(buffer, 0, jsonBytesLength);
        } else if (this.type == TYPE_MUTATION) {
            this.mutation = new Mutation();
            this.mutation.readFields(in);
        } else {
            throw new IOException("Invalid type: " + this.type);
        }
    }

    public Mutation getMutation() {
        return mutation;
    }

    public int getType() {
        return type;
    }

    public String getId() {
        return id;
    }

    public String getJson() {
        return json;
    }
}
