package org.elasticsearch.plugin.spliter;

import org.elasticsearch.common.inject.Binder;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.index.spliter.AddSpliterAction;
import org.elasticsearch.index.spliter.ExecuteJobAction;
import org.elasticsearch.index.spliter.GetSpliterAction;

/**
 * @author xingtianyu(code4j) Created on 2018-2-25.
 */
public class SplitModule implements Module{

    @Override
    public void configure(Binder binder) {
        binder.bind(GetSpliterAction.class).asEagerSingleton();
        binder.bind(AddSpliterAction.class).asEagerSingleton();
        binder.bind(ExecuteJobAction.class).asEagerSingleton();
    }
}
