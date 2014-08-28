package org.pentaho.mongo.wrapper;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;

import org.pentaho.di.core.Const;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.trans.steps.mongodb.MongoDbMeta;

public class MongoClientWrapperFactory {

    public static MongoClientWrapper createMongoClientWrapper(MongoDbMeta meta, VariableSpace vars,
                                                              LogChannelInterface log) throws KettleException {

        if (meta.getUseKerberosAuthentication()) {
            KerberosMongoClientWrapper wrapper = new KerberosMongoClientWrapper(meta, vars, log);
            ClassLoader loader = wrapper.getClass().getClassLoader();
            Class<?>[] interfaces = new Class<?>[]{MongoClientWrapper.class};
            InvocationHandler h = new KerberosInvocationHandler(wrapper.getAuthContext(), wrapper);
            return (MongoClientWrapper) Proxy.newProxyInstance(loader, interfaces, h);
        } else {
            String user = vars.environmentSubstitute(meta.getAuthenticationUser());
            String password = vars.environmentSubstitute(meta.getAuthenticationPassword());
            if (!Const.isEmpty(user) || !Const.isEmpty(password)) {
                return new UsernamePasswordMongoClientWrapper(meta, vars, log);
            } else {
                return new NoAuthMongoClientWrapper(meta, vars, log);
            }
        }
    }

}
