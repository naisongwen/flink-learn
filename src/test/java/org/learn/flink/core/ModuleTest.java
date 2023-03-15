package org.learn.flink.core;

import org.apache.flink.table.module.CoreModuleFactory;
import org.apache.flink.table.module.ModuleEntry;
import org.apache.flink.table.module.ModuleManager;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;

public class ModuleTest {
    private ModuleManager manager;

    @Before
    public void before() {
        manager = new ModuleManager();
    }

    @Test
    public void testModule(){
        // CoreModule is loaded by default
        List<ModuleEntry> moduleEntryList=manager.listFullModules();
        //name -> new ModuleEntry(name, true)
        List<String> names=moduleEntryList.stream()
                .map(e -> e.name())
                .collect(Collectors.toList());
        System.out.println(String.join("，",names));
        List<String> moduleList=manager.listModules();
        System.out.println(String.join("，",moduleList));
    }
}
