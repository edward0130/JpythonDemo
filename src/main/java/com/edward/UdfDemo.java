package com.edward;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.MapredContext;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import org.python.core.Py;
import org.python.core.PyObject;
import org.python.util.PythonInterpreter;

import java.io.IOException;
import java.io.InputStream;


/*
    create function pydemo as 'com.edward.UdfDemo'
    using jar 'hdfs://hacluster/tmp/JpythonDemo-1.0-SNAPSHOT-jar-with-dependencies.jar',
    file 'hdfs://hacluster/tmp/test.py';

    drop function pydemo;

    test.py
    # coding=utf-8
    import math;
    class Calculator(object):
        def power(self, x, y):
                return math.pow(x,y)
 */
public class UdfDemo extends GenericUDF {

    private  PythonInterpreter pi = null;
    private  PyObject pyObject = null;

    private  String file = null;

    private FSDataInputStream fs = null;

    @Override
    public ObjectInspector initialize(ObjectInspector[] objectInspectors) throws UDFArgumentException {


        try {
            //创建inputstream
            FileSystem fileSystem = FileSystem.get(new Configuration());
            fs = fileSystem.open(new Path("hdfs://hacluster/tmp/test.py"));

            //InputStream inputStream = this.getClass().getClassLoader().getResourceAsStream("./test.py");
            //文件
            //String pythonFile = "test.py";
            //python类名
            String pyClassName = "Calculator";
            //python类实例名
            String pyObjName = "cal";

            pi = new PythonInterpreter();
            //pi.execfile(pythonFile);
            pi.execfile(fs);
            pi.exec(pyObjName+"="+pyClassName+"()");
            pyObject = pi.get(pyObjName);

        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return PrimitiveObjectInspectorFactory.javaDoubleObjectInspector;
//        return PrimitiveObjectInspectorFactory.javaStringObjectInspector;
    }

    @Override
    protected String getFuncName() {
        return StringUtils.substring(this.getClass().getSimpleName(), 0,10).toLowerCase();
    }

    @Override
    public Object evaluate(DeferredObject[] deferredObjects) throws HiveException {
        String a = deferredObjects[0].get().toString();
        String b = deferredObjects[1].get().toString();
        PyObject result = pyObject.invoke("power", new PyObject[]{Py.newInteger(Integer.parseInt(a)), Py.newInteger(Integer.parseInt(b))});
        double total = Py.py2double(result);
        return total;
    }

    @Override
    public void configure(MapredContext context) {
        super.configure(context);

    }

    @Override
    public String getDisplayString(String[] strings) {
        return "--";
    }

    @Override
    public void close() throws IOException {
        super.close();
        fs.close();
        pi.cleanup();
        pi.close();
    }
}
