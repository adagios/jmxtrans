package com.googlecode.jmxtrans.model.output;

import com.googlecode.jmxtrans.model.Query;
import com.googlecode.jmxtrans.model.Result;
import com.googlecode.jmxtrans.util.LifecycleException;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.UnknownHostException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Writes out data in the same format as the OpenTSDB, except to a file
 * and tab delimited. Takes advantage of Log4J RollingFileAppender to
 * automatically handle rolling the files after they reach a certain size.
 * <p/>
 * The default max size of the log files are 10MB (maxLogFileSize) The default
 * number of rolled files to keep is 200 (maxLogBackupFiles)
 *
 * @author jon
 */
public class OpenTSDBKeyOutWriter extends KeyOutWriter {
    private static final Logger log = LoggerFactory.getLogger(OpenTSDBKeyOutWriter.class);
    private Map<String, String> tags;
    private String tagName;

    String addTags(String resultString) throws UnknownHostException {
        resultString = addTag(resultString, "host", java.net.InetAddress.getLocalHost().getHostName());
        if (tags != null)
            for (Map.Entry<String, String> tagEntry : tags.entrySet()) {
                resultString = addTag(resultString, tagEntry.getKey(), tagEntry.getValue());
            }
        return resultString;
    }

    String addTag(String resultString, String tagName, String tagValue) {
        String tagFormat = " %s=%s";
        resultString += String.format(tagFormat, tagName, tagValue);
        return resultString;
    }

    String getResultString(String className, String attributeName, long epoch, Object value) {
        String resultStringFormat = "%s.%s %d %s";
        return String.format(resultStringFormat, className, attributeName, epoch, value);
    }

    String getResultString(String className, String attributeName, long epoch, Object value, String tagName, String tagValue) {
        String taggedResultStringFormat = "%s.%s %d %s %s=%s";
        return String.format(taggedResultStringFormat, className, attributeName, epoch, value, tagName, tagValue);
    }

    List<String> resultParser(Result result) throws UnknownHostException {
        List<String> resultStrings = new LinkedList<String>();
        Map<String, Object> values = result.getValues();
        if (values == null)
            return resultStrings;

        String attributeName = result.getAttributeName();
        String className = result.getClassNameAlias() == null ? result.getClassName() : result.getClassNameAlias();
        if (values.containsKey(attributeName) && values.size() == 1) {
            String resultString = getResultString(className, attributeName, (long) (result.getEpoch() / 1000L), values.get(attributeName));
            resultString = addTags(resultString);
            if (getTypeNames().size() > 0) {
                resultString = addTag(resultString, StringUtils.join(getTypeNames(), ""), getConcatedTypeNameValues(result.getTypeName()));
            }
            resultStrings.add(resultString);
        } else {
            for (Map.Entry<String, Object> valueEntry : values.entrySet()) {
                String resultString = getResultString(className, attributeName, (long) (result.getEpoch() / 1000L), valueEntry.getValue(), tagName, valueEntry.getKey());
                resultString = addTags(resultString);
                if (getTypeNames().size() > 0) {
                    resultString = addTag(resultString, StringUtils.join(getTypeNames(), ""), getConcatedTypeNameValues(result.getTypeName()));
                }
                resultStrings.add(resultString);
            }
        }
        return resultStrings;
    }

    @Override
    public void doWrite(Query query) throws Exception {

        for (Result result : query.getResults()) {
            for (String resultString : resultParser(result)) {
                if (isDebugEnabled())
                    System.out.println(resultString);

                logger.info(resultString);
            }
        }
    }

    @Override
    public void start() throws LifecycleException {
        tags = (Map<String, String>) this.getSettings().get("tags");
        tagName = this.getStringSetting("tagName", "type");
    }

}
