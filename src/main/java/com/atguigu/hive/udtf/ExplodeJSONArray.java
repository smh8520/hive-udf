package com.atguigu.hive.udtf;


import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.json.JSONArray;

import java.util.ArrayList;
import java.util.List;

/**
 * @author smh
 * @create 2021-06-14 11:03
 */
public class ExplodeJSONArray extends GenericUDTF {
    @Override
    public StructObjectInspector initialize(StructObjectInspector argOIs) throws UDFArgumentException {
       if(argOIs.getAllStructFieldRefs().size()!=1){
           throw new UDFArgumentLengthException("length!=1");
       }

        String typeName = argOIs.getAllStructFieldRefs().get(0).getFieldObjectInspector().getTypeName();
       if(!typeName.equals("string")){
            throw new UDFArgumentTypeException(0,"类型不对");
       }

        List<String> fieldName=new ArrayList<>();
        List<ObjectInspector> fieldOIs=new ArrayList<>();
        fieldName.add("item");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldName,fieldOIs);
    }

    @Override
    public void process(Object[] objects) throws HiveException {
        //对传入的参数做操作
        //获取json数组字符串
        String jsonStrArr = objects[0].toString();
        //转换为json数组
        JSONArray jsonArray = new JSONArray(jsonStrArr);
        for (int i = 0; i < jsonArray.length(); i++) {
            //获取json数组中的每个元素
            String s = jsonArray.getString(i);
            String[] strings = new String[1];
            strings[0] = s;
            forward(strings);
        }
    }

    @Override
    public void close() throws HiveException {

    }
}
