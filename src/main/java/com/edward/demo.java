package com.edward;

import org.python.core.Py;
import org.python.core.PyObject;
import org.python.util.PythonInterpreter;

public class demo {

    public static void main(String[] args) {

        String pythonFile = "e:\\test.py";

        //python类名
        String pyClassName = "Calculator";
        //python类实例名
        String pyObjName = "cal";

        PythonInterpreter pi = new PythonInterpreter();
        pi.execfile(pythonFile);
        //pi.execfile();
        pi.exec(pyObjName+"="+pyClassName+"()");
        PyObject pyObject = pi.get(pyObjName);
        PyObject result = pyObject.invoke("power", new PyObject[]{Py.newInteger(2), Py.newInteger(3)});
        double v = Py.py2double(result);
        System.out.println(v);
        pi.cleanup();
        pi.close();
    }
}
