package Utils;

import java.io.Serializable;
import java.time.Instant;
import java.util.*;

public class AdditionalInfo implements Serializable {
    private final Set<String> uniq = new HashSet<>();
    private final List<Long> processingTime = new ArrayList<>();
    private Long msgCount = 0L;

    public AdditionalInfo registerString(InputData inputData) {
        uniq.add(inputData.getCountryName());
        processingTime.add(Date.from(Instant.now()).getTime() - inputData.getTimeCreated());
        msgCount++;
        return this;
    }

    @Override
    public String toString() {
        return "AdditionalInfo{" +
                "uniq=" + getUniqCounter() +
                ", min processingTime=" + getMinProcessingTime() +
                ", max processingTime=" + getMaxProcessingTime() +
                String.format(", avg processingTime=%1$.5f", gerAvgProcessingTime())+
                ", msgCount=" + getMsgCount() +
                '}';
    }

    public long getUniqCounter() {
        return uniq.size();
    }

    public long getMaxProcessingTime() {
        return Collections.max(processingTime);
    }

    public double gerAvgProcessingTime(){
        OptionalDouble average = processingTime
                .stream()
                .mapToDouble(a -> a)
                .average();
        return average.isPresent() ? average.getAsDouble() : 0;
    }

    public long getMinProcessingTime() {
        return Collections.min(processingTime);
    }

    public long getMsgCount() {
        return msgCount;
    }


}
