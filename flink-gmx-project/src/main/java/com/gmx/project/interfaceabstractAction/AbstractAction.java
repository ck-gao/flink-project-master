package com.gmx.project.interfaceabstractAction;


import org.apache.kafka.common.requests.RequestContext;

import java.awt.*;

/**
 * Java提供和支持创建抽象类和接口。它们的实现有共同点，不同点在于：
 *
 * 1.接口中所有的方法隐含的都是抽象的。而抽象类则可以同时包含抽象和非抽象的方法。
 * 2.类可以实现很多个接口，但是只能继承一个抽象类
 * 3.类可以不实现抽象类和接口声明的所有方法，当然，在这种情况下，类也必须得声明成是抽象的。
 * 4.抽象类可以在不提供接口方法实现的情况下实现接口。
 * 5.Java接口中声明的变量默认都是final的。抽象类可以包含非final的变量。
 * 6.Java接口中的成员函数默认是public的。抽象类的成员函数可以是private，protected或者是public。
 * 7.接口是绝对抽象的，不可以被实例化，抽象类也不可以被实例化。
 * 8.一个类实现接口的话要实现接口的所有方法，而抽象类不一定
 *
 * 一句话总结：
 * 从设计层面来说，抽象是对类的抽象，是一种模板设计，接口是行为的抽象，是一种行为的规范。
 */
  //test4
public abstract class AbstractAction  implements Action {

    //抽象类中可以实现某个方法
    public final Event execute(RequestContext context) throws Exception {
        Event result = doPreExecute(context);
        if (result == null) {
            result = doExecute(context);
            doPostExecute(context);
        } else {
            if (true) {
            }
        }
        return result;
    }

    protected Event doPreExecute(RequestContext context) throws Exception {
        return null;
    }

    //抽象方法  抽象类abstract中声明  可以不实现
    protected abstract Event doExecute(RequestContext context) throws Exception;

    //抽象类实现了Action接口，需要实现接口中的方法，如果不实现可以在方法上加abstract修饰
    protected Event doExecute1(RequestContext context) throws Exception {
        return null;
    }


    protected void doPostExecute(RequestContext context) throws Exception {
    }

}
