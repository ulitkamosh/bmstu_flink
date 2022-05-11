package Utils;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.flink.shaded.netty4.io.netty.util.internal.StringUtil;

import java.util.ArrayList;

public class InputData {

    int id;
    String CountryName;

    public InputData(int id, String countryName) {
        this.id = id;
        CountryName = countryName;
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
