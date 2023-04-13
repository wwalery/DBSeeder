package dev.walgo.dbseeder.writer;

import dev.walgo.dbseeder.data.SeedInfo;
import org.apache.commons.lang3.tuple.Pair;

public interface IWriter {

    Pair<Integer, Integer> write(SeedInfo info);

}
