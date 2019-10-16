sbt -Dconfig.resource=queryprocessor1.conf "point-stream-impl/runMain com.earthwave.pointstream.impl.PointWorkerProcess -instance=1 -worker=QueryProcessor" &
P1=$!
sbt -Dconfig.resource=swathProcessor1.conf "point-stream-impl/runMain com.earthwave.pointstream.impl.PointWorkerProcess -instance=1 -worker=SwathProcessor" &
P5=$!
sbt -Dconfig.resource=publishprocessor1.conf "point-stream-impl/runMain com.earthwave.pointstream.impl.PointWorkerProcess -instance=1 -worker=PublisherProcessor" &
P9=$!

wait $P1 $P5 $P9


