package org.elasticsearch.plugin.spliter;

import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.Binder;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.index.spliter.RestSpliterAction;

/**
 * @author xingtianyu(code4j) Created on 2018-2-25.
 */
public class SplitModule implements Module{

    @Override
    public void configure(Binder binder) {
        binder.bind(RestSpliterAction.class).asEagerSingleton();

    }
}
