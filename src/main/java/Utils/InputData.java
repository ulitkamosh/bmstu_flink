package Utils;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.flink.shaded.netty4.io.netty.util.internal.StringUtil;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;

public class InputData {

    int id;
    String CountryName;
    long timeCreated;

    public InputData(int id, String countryName) {
        this.id = id;
        CountryName = countryName;
        timeCreated = Date.from(Instant.now()).getTime();
    }

    public long getTimeCreated() {
        return timeCreated;
    }

    public int getId() {
        return id;
    }

    public String getCountryName() {
        return CountryName;
    }

    public static InputData getDataObject(String inputStr) {
        // Split the string
        String[] attributes = inputStr
                .replace("[", "").replace("]", "").replace(" ", "")
                .split(",");

        // Ignore empty strings
        if(StringUtil.isNullOrEmpty(attributes[0]))
            attributes = ArrayUtils.remove(attributes,0);

        return new InputData(Integer.parseInt(attributes[0]), attributes[1]);
    }

}
