package org.elasticsearch.plugin.spliter;

import org.elasticsearch.SpecialPermission;
import org.elasticsearch.common.inject.Binder;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.inject.TypeLiteral;
import org.elasticsearch.index.spliter.*;

import java.security.AccessController;
import java.security.PrivilegedAction;

/**
 * @author xingtianyu(code4j) Created on 2018-2-25.
 */
public class SplitModule implements Module{

    @Override
    public void configure(Binder binder) {
        binder.bind(GetSpliterAction.class).asEagerSingleton();
        binder.bind(AddSpliterAction.class).asEagerSingleton();
        binder.bind(ExecuteJobAction.class).asEagerSingleton();
        binder.bind(PauseJobAction.class).asEagerSingleton();
        binder.bind(ResumeJobAction.class).asEagerSingleton();
    }
}
