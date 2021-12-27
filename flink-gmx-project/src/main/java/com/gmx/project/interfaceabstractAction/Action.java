package com.gmx.project.interfaceabstractAction;

import org.apache.kafka.common.requests.RequestContext;

import java.awt.*;

public interface Action {
    public Event execute(RequestContext context) throws Exception;

    public Event execute1(RequestContext context) throws Exception;
}
