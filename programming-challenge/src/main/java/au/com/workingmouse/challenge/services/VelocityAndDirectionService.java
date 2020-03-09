package au.com.workingmouse.challenge.services;

import au.com.workingmouse.challenge.models.VelocityAndDirectionData;
import org.apache.log4j.Logger;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.*;

public class VelocityAndDirectionService {

    private static final Logger LOGGER = Logger.getLogger(VelocityAndDirectionService.class);

    private static Integer recordList = 0;
    private static Integer dcsModList = 0;
    private static Integer dcsSerialList = 0;
    private static Double dcsAbsspdAvgList = 0.0;
    private static Double dcsDirectionAvgList = 0.0;
    private static Double dcsNorthCurAvgList = 0.0;
    private static Double dcsEastCurAvgList = 0.0;
    private static Double dcsHeadingAvgList = 0.0;
    private static Double dcsTiltXAvgList = 0.0;
    private static Double dcsTiltYAvgList = 0.0;
    private static Double dcsSpStdAvgList = 0.0;
    private static Double dcsSigStrengthAvgList = 0.0;
    private static Double dcsPingCntAvgList = 0.0;
    private static Double dcsAbsTiltAvgList = 0.0;
    private static Double dscMaxTiltAvgList = 0.0;
    private static Double dcsStdTiltAvgList = 0.0;


    public static VelocityAndDirectionData parseLine(String line) {
        // NOTE: This CSV parsing is not fully inclusive
        line = line.replace("\"","");
        String[] parts = line.split(",");

        VelocityAndDirectionData velocityAndDirectionData = new VelocityAndDirectionData();

        if (parts[0].length() <= 16) {
            velocityAndDirectionData.setTimestamp(Timestamp.valueOf(parts[0] + ":00"));
        } else {
            velocityAndDirectionData.setTimestamp(Timestamp.valueOf(parts[0]));
        }
        velocityAndDirectionData.setRecord(Integer.parseInt(parts[1]));
        if (parts.length == 17) {
            velocityAndDirectionData.setDcsModel(Integer.parseInt(parts[2]));
            velocityAndDirectionData.setDcsSerial(Integer.parseInt(parts[3]));
            velocityAndDirectionData.setDcsAbsspdAvg(Double.parseDouble(parts[4]));
            velocityAndDirectionData.setDcsDirectionAvg(Double.parseDouble(parts[5]));
            velocityAndDirectionData.setDcsNorthCurAvg(Double.parseDouble(parts[6]));
            velocityAndDirectionData.setDcsEastCurAvg(Double.parseDouble(parts[7]));
            velocityAndDirectionData.setDcsHeadingAvg(Double.parseDouble(parts[8]));
            velocityAndDirectionData.setDcsTiltXAvg(Double.parseDouble(parts[9]));
            velocityAndDirectionData.setDcsTiltYAvg(Double.parseDouble(parts[10]));
            velocityAndDirectionData.setDcsSpStdAvg(Double.parseDouble(parts[11]));
            velocityAndDirectionData.setDcsSigStrengthAvg(Double.parseDouble(parts[12]));
            velocityAndDirectionData.setDcsPingCntAvg(Double.parseDouble(parts[13]));
            velocityAndDirectionData.setDcsAbsTiltAvg(Double.parseDouble(parts[14]));
            velocityAndDirectionData.setDscMaxTiltAvg(Double.parseDouble(parts[15]));
            velocityAndDirectionData.setDcsStdTiltAvg(Double.parseDouble(parts[16]));
        }

        else {
            switch(parts.length) {
                case 2:
                    velocityAndDirectionData.setDcsModel(0);
                    velocityAndDirectionData.setDcsSerial(0);
                    velocityAndDirectionData.setDcsAbsspdAvg(0.0);
                    velocityAndDirectionData.setDcsDirectionAvg(0.0);
                    velocityAndDirectionData.setDcsNorthCurAvg(0.0);
                    velocityAndDirectionData.setDcsEastCurAvg(0.0);
                    velocityAndDirectionData.setDcsHeadingAvg(0.0);
                    velocityAndDirectionData.setDcsTiltXAvg(0.0);
                    velocityAndDirectionData.setDcsTiltYAvg(0.0);
                    velocityAndDirectionData.setDcsSpStdAvg(0.0);
                    velocityAndDirectionData.setDcsSigStrengthAvg(0.0);
                    velocityAndDirectionData.setDcsPingCntAvg(0.0);
                    velocityAndDirectionData.setDcsAbsTiltAvg(0.0);
                    velocityAndDirectionData.setDscMaxTiltAvg(0.0);
                    velocityAndDirectionData.setDcsStdTiltAvg(0.0);
                    break;
                case 3:
                    velocityAndDirectionData.setDcsModel(Integer.parseInt(parts[2]));
                    velocityAndDirectionData.setDcsSerial(0);
                    velocityAndDirectionData.setDcsAbsspdAvg(0.0);
                    velocityAndDirectionData.setDcsDirectionAvg(0.0);
                    velocityAndDirectionData.setDcsNorthCurAvg(0.0);
                    velocityAndDirectionData.setDcsEastCurAvg(0.0);
                    velocityAndDirectionData.setDcsHeadingAvg(0.0);
                    velocityAndDirectionData.setDcsTiltXAvg(0.0);
                    velocityAndDirectionData.setDcsTiltYAvg(0.0);
                    velocityAndDirectionData.setDcsSpStdAvg(0.0);
                    velocityAndDirectionData.setDcsSigStrengthAvg(0.0);
                    velocityAndDirectionData.setDcsPingCntAvg(0.0);
                    velocityAndDirectionData.setDcsAbsTiltAvg(0.0);
                    velocityAndDirectionData.setDscMaxTiltAvg(0.0);
                    velocityAndDirectionData.setDcsStdTiltAvg(0.0);
                    break;
                case 4:
                    velocityAndDirectionData.setDcsModel(Integer.parseInt(parts[2]));
                    velocityAndDirectionData.setDcsSerial(Integer.parseInt(parts[3]));
                    velocityAndDirectionData.setDcsAbsspdAvg(0.0);
                    velocityAndDirectionData.setDcsDirectionAvg(0.0);
                    velocityAndDirectionData.setDcsNorthCurAvg(0.0);
                    velocityAndDirectionData.setDcsEastCurAvg(0.0);
                    velocityAndDirectionData.setDcsHeadingAvg(0.0);
                    velocityAndDirectionData.setDcsTiltXAvg(0.0);
                    velocityAndDirectionData.setDcsTiltYAvg(0.0);
                    velocityAndDirectionData.setDcsSpStdAvg(0.0);
                    velocityAndDirectionData.setDcsSigStrengthAvg(0.0);
                    velocityAndDirectionData.setDcsPingCntAvg(0.0);
                    velocityAndDirectionData.setDcsAbsTiltAvg(0.0);
                    velocityAndDirectionData.setDscMaxTiltAvg(0.0);
                    velocityAndDirectionData.setDcsStdTiltAvg(0.0);
                    break;
                case 5:
                    velocityAndDirectionData.setDcsModel(Integer.parseInt(parts[2]));
                    velocityAndDirectionData.setDcsSerial(Integer.parseInt(parts[3]));
                    velocityAndDirectionData.setDcsAbsspdAvg(Double.parseDouble(parts[4]));
                    velocityAndDirectionData.setDcsDirectionAvg(0.0);
                    velocityAndDirectionData.setDcsNorthCurAvg(0.0);
                    velocityAndDirectionData.setDcsEastCurAvg(0.0);
                    velocityAndDirectionData.setDcsHeadingAvg(0.0);
                    velocityAndDirectionData.setDcsTiltXAvg(0.0);
                    velocityAndDirectionData.setDcsTiltYAvg(0.0);
                    velocityAndDirectionData.setDcsSpStdAvg(0.0);
                    velocityAndDirectionData.setDcsSigStrengthAvg(0.0);
                    velocityAndDirectionData.setDcsPingCntAvg(0.0);
                    velocityAndDirectionData.setDcsAbsTiltAvg(0.0);
                    velocityAndDirectionData.setDscMaxTiltAvg(0.0);
                    velocityAndDirectionData.setDcsStdTiltAvg(0.0);
                    break;
                case 6:
                    velocityAndDirectionData.setDcsModel(Integer.parseInt(parts[2]));
                    velocityAndDirectionData.setDcsSerial(Integer.parseInt(parts[3]));
                    velocityAndDirectionData.setDcsAbsspdAvg(Double.parseDouble(parts[4]));
                    velocityAndDirectionData.setDcsDirectionAvg(Double.parseDouble(parts[5]));
                    velocityAndDirectionData.setDcsNorthCurAvg(0.0);
                    velocityAndDirectionData.setDcsEastCurAvg(0.0);
                    velocityAndDirectionData.setDcsHeadingAvg(0.0);
                    velocityAndDirectionData.setDcsTiltXAvg(0.0);
                    velocityAndDirectionData.setDcsTiltYAvg(0.0);
                    velocityAndDirectionData.setDcsSpStdAvg(0.0);
                    velocityAndDirectionData.setDcsSigStrengthAvg(0.0);
                    velocityAndDirectionData.setDcsPingCntAvg(0.0);
                    velocityAndDirectionData.setDcsAbsTiltAvg(0.0);
                    velocityAndDirectionData.setDscMaxTiltAvg(0.0);
                    velocityAndDirectionData.setDcsStdTiltAvg(0.0);
                    break;
                case 7:
                    velocityAndDirectionData.setDcsModel(Integer.parseInt(parts[2]));
                    velocityAndDirectionData.setDcsSerial(Integer.parseInt(parts[3]));
                    velocityAndDirectionData.setDcsAbsspdAvg(Double.parseDouble(parts[4]));
                    velocityAndDirectionData.setDcsDirectionAvg(Double.parseDouble(parts[5]));
                    velocityAndDirectionData.setDcsNorthCurAvg(Double.parseDouble(parts[6]));
                    velocityAndDirectionData.setDcsEastCurAvg(0.0);
                    velocityAndDirectionData.setDcsHeadingAvg(0.0);
                    velocityAndDirectionData.setDcsTiltXAvg(0.0);
                    velocityAndDirectionData.setDcsTiltYAvg(0.0);
                    velocityAndDirectionData.setDcsSpStdAvg(0.0);
                    velocityAndDirectionData.setDcsSigStrengthAvg(0.0);
                    velocityAndDirectionData.setDcsPingCntAvg(0.0);
                    velocityAndDirectionData.setDcsAbsTiltAvg(0.0);
                    velocityAndDirectionData.setDscMaxTiltAvg(0.0);
                    velocityAndDirectionData.setDcsStdTiltAvg(0.0);
                    break;
                case 8:
                    velocityAndDirectionData.setDcsModel(Integer.parseInt(parts[2]));
                    velocityAndDirectionData.setDcsSerial(Integer.parseInt(parts[3]));
                    velocityAndDirectionData.setDcsAbsspdAvg(Double.parseDouble(parts[4]));
                    velocityAndDirectionData.setDcsDirectionAvg(Double.parseDouble(parts[5]));
                    velocityAndDirectionData.setDcsNorthCurAvg(Double.parseDouble(parts[6]));
                    velocityAndDirectionData.setDcsEastCurAvg(Double.parseDouble(parts[7]));
                    velocityAndDirectionData.setDcsHeadingAvg(0.0);
                    velocityAndDirectionData.setDcsTiltXAvg(0.0);
                    velocityAndDirectionData.setDcsTiltYAvg(0.0);
                    velocityAndDirectionData.setDcsSpStdAvg(0.0);
                    velocityAndDirectionData.setDcsSigStrengthAvg(0.0);
                    velocityAndDirectionData.setDcsPingCntAvg(0.0);
                    velocityAndDirectionData.setDcsAbsTiltAvg(0.0);
                    velocityAndDirectionData.setDscMaxTiltAvg(0.0);
                    velocityAndDirectionData.setDcsStdTiltAvg(0.0);
                    break;
                case 9:
                    velocityAndDirectionData.setDcsModel(Integer.parseInt(parts[2]));
                    velocityAndDirectionData.setDcsSerial(Integer.parseInt(parts[3]));
                    velocityAndDirectionData.setDcsAbsspdAvg(Double.parseDouble(parts[4]));
                    velocityAndDirectionData.setDcsDirectionAvg(Double.parseDouble(parts[5]));
                    velocityAndDirectionData.setDcsNorthCurAvg(Double.parseDouble(parts[6]));
                    velocityAndDirectionData.setDcsEastCurAvg(Double.parseDouble(parts[7]));
                    velocityAndDirectionData.setDcsHeadingAvg(Double.parseDouble(parts[8]));
                    velocityAndDirectionData.setDcsTiltXAvg(0.0);
                    velocityAndDirectionData.setDcsTiltYAvg(0.0);
                    velocityAndDirectionData.setDcsSpStdAvg(0.0);
                    velocityAndDirectionData.setDcsSigStrengthAvg(0.0);
                    velocityAndDirectionData.setDcsPingCntAvg(0.0);
                    velocityAndDirectionData.setDcsAbsTiltAvg(0.0);
                    velocityAndDirectionData.setDscMaxTiltAvg(0.0);
                    velocityAndDirectionData.setDcsStdTiltAvg(0.0);
                    break;
                case 10:
                    velocityAndDirectionData.setDcsModel(Integer.parseInt(parts[2]));
                    velocityAndDirectionData.setDcsSerial(Integer.parseInt(parts[3]));
                    velocityAndDirectionData.setDcsAbsspdAvg(Double.parseDouble(parts[4]));
                    velocityAndDirectionData.setDcsDirectionAvg(Double.parseDouble(parts[5]));
                    velocityAndDirectionData.setDcsNorthCurAvg(Double.parseDouble(parts[6]));
                    velocityAndDirectionData.setDcsEastCurAvg(Double.parseDouble(parts[7]));
                    velocityAndDirectionData.setDcsHeadingAvg(Double.parseDouble(parts[8]));
                    velocityAndDirectionData.setDcsTiltXAvg(Double.parseDouble(parts[9]));
                    velocityAndDirectionData.setDcsTiltYAvg(0.0);
                    velocityAndDirectionData.setDcsSpStdAvg(0.0);
                    velocityAndDirectionData.setDcsSigStrengthAvg(0.0);
                    velocityAndDirectionData.setDcsPingCntAvg(0.0);
                    velocityAndDirectionData.setDcsAbsTiltAvg(0.0);
                    velocityAndDirectionData.setDscMaxTiltAvg(0.0);
                    velocityAndDirectionData.setDcsStdTiltAvg(0.0);
                    break;
                case 11:
                    velocityAndDirectionData.setDcsModel(Integer.parseInt(parts[2]));
                    velocityAndDirectionData.setDcsSerial(Integer.parseInt(parts[3]));
                    velocityAndDirectionData.setDcsAbsspdAvg(Double.parseDouble(parts[4]));
                    velocityAndDirectionData.setDcsDirectionAvg(Double.parseDouble(parts[5]));
                    velocityAndDirectionData.setDcsNorthCurAvg(Double.parseDouble(parts[6]));
                    velocityAndDirectionData.setDcsEastCurAvg(Double.parseDouble(parts[7]));
                    velocityAndDirectionData.setDcsHeadingAvg(Double.parseDouble(parts[8]));
                    velocityAndDirectionData.setDcsTiltXAvg(Double.parseDouble(parts[9]));
                    velocityAndDirectionData.setDcsTiltYAvg(Double.parseDouble(parts[10]));
                    velocityAndDirectionData.setDcsSpStdAvg(0.0);
                    velocityAndDirectionData.setDcsSigStrengthAvg(0.0);
                    velocityAndDirectionData.setDcsPingCntAvg(0.0);
                    velocityAndDirectionData.setDcsAbsTiltAvg(0.0);
                    velocityAndDirectionData.setDscMaxTiltAvg(0.0);
                    velocityAndDirectionData.setDcsStdTiltAvg(0.0);
                    break;
                case 12:
                    velocityAndDirectionData.setDcsModel(Integer.parseInt(parts[2]));
                    velocityAndDirectionData.setDcsSerial(Integer.parseInt(parts[3]));
                    velocityAndDirectionData.setDcsAbsspdAvg(Double.parseDouble(parts[4]));
                    velocityAndDirectionData.setDcsDirectionAvg(Double.parseDouble(parts[5]));
                    velocityAndDirectionData.setDcsNorthCurAvg(Double.parseDouble(parts[6]));
                    velocityAndDirectionData.setDcsEastCurAvg(Double.parseDouble(parts[7]));
                    velocityAndDirectionData.setDcsHeadingAvg(Double.parseDouble(parts[8]));
                    velocityAndDirectionData.setDcsTiltXAvg(Double.parseDouble(parts[9]));
                    velocityAndDirectionData.setDcsTiltYAvg(Double.parseDouble(parts[10]));
                    velocityAndDirectionData.setDcsSpStdAvg(Double.parseDouble(parts[11]));
                    velocityAndDirectionData.setDcsSigStrengthAvg(0.0);
                    velocityAndDirectionData.setDcsPingCntAvg(0.0);
                    velocityAndDirectionData.setDcsAbsTiltAvg(0.0);
                    velocityAndDirectionData.setDscMaxTiltAvg(0.0);
                    velocityAndDirectionData.setDcsStdTiltAvg(0.0);
                    break;
                case 13:
                    velocityAndDirectionData.setDcsModel(Integer.parseInt(parts[2]));
                    velocityAndDirectionData.setDcsSerial(Integer.parseInt(parts[3]));
                    velocityAndDirectionData.setDcsAbsspdAvg(Double.parseDouble(parts[4]));
                    velocityAndDirectionData.setDcsDirectionAvg(Double.parseDouble(parts[5]));
                    velocityAndDirectionData.setDcsNorthCurAvg(Double.parseDouble(parts[6]));
                    velocityAndDirectionData.setDcsEastCurAvg(Double.parseDouble(parts[7]));
                    velocityAndDirectionData.setDcsHeadingAvg(Double.parseDouble(parts[8]));
                    velocityAndDirectionData.setDcsTiltXAvg(Double.parseDouble(parts[9]));
                    velocityAndDirectionData.setDcsTiltYAvg(Double.parseDouble(parts[10]));
                    velocityAndDirectionData.setDcsSpStdAvg(Double.parseDouble(parts[11]));
                    velocityAndDirectionData.setDcsSigStrengthAvg(Double.parseDouble(parts[12]));
                    velocityAndDirectionData.setDcsPingCntAvg(0.0);
                    velocityAndDirectionData.setDcsAbsTiltAvg(0.0);
                    velocityAndDirectionData.setDscMaxTiltAvg(0.0);
                    velocityAndDirectionData.setDcsStdTiltAvg(0.0);
                    break;
                case 14:
                    velocityAndDirectionData.setDcsModel(Integer.parseInt(parts[2]));
                    velocityAndDirectionData.setDcsSerial(Integer.parseInt(parts[3]));
                    velocityAndDirectionData.setDcsAbsspdAvg(Double.parseDouble(parts[4]));
                    velocityAndDirectionData.setDcsDirectionAvg(Double.parseDouble(parts[5]));
                    velocityAndDirectionData.setDcsNorthCurAvg(Double.parseDouble(parts[6]));
                    velocityAndDirectionData.setDcsEastCurAvg(Double.parseDouble(parts[7]));
                    velocityAndDirectionData.setDcsHeadingAvg(Double.parseDouble(parts[8]));
                    velocityAndDirectionData.setDcsTiltXAvg(Double.parseDouble(parts[9]));
                    velocityAndDirectionData.setDcsTiltYAvg(Double.parseDouble(parts[10]));
                    velocityAndDirectionData.setDcsSpStdAvg(Double.parseDouble(parts[11]));
                    velocityAndDirectionData.setDcsSigStrengthAvg(Double.parseDouble(parts[12]));
                    velocityAndDirectionData.setDcsPingCntAvg(Double.parseDouble(parts[13]));
                    velocityAndDirectionData.setDcsAbsTiltAvg(0.0);
                    velocityAndDirectionData.setDscMaxTiltAvg(0.0);
                    velocityAndDirectionData.setDcsStdTiltAvg(0.0);
                    break;
                case 15:
                    velocityAndDirectionData.setDcsModel(Integer.parseInt(parts[2]));
                    velocityAndDirectionData.setDcsSerial(Integer.parseInt(parts[3]));
                    velocityAndDirectionData.setDcsAbsspdAvg(Double.parseDouble(parts[4]));
                    velocityAndDirectionData.setDcsDirectionAvg(Double.parseDouble(parts[5]));
                    velocityAndDirectionData.setDcsNorthCurAvg(Double.parseDouble(parts[6]));
                    velocityAndDirectionData.setDcsEastCurAvg(Double.parseDouble(parts[7]));
                    velocityAndDirectionData.setDcsHeadingAvg(Double.parseDouble(parts[8]));
                    velocityAndDirectionData.setDcsTiltXAvg(Double.parseDouble(parts[9]));
                    velocityAndDirectionData.setDcsTiltYAvg(Double.parseDouble(parts[10]));
                    velocityAndDirectionData.setDcsSpStdAvg(Double.parseDouble(parts[11]));
                    velocityAndDirectionData.setDcsSigStrengthAvg(Double.parseDouble(parts[12]));
                    velocityAndDirectionData.setDcsPingCntAvg(Double.parseDouble(parts[13]));
                    velocityAndDirectionData.setDcsAbsTiltAvg(Double.parseDouble(parts[14]));
                    velocityAndDirectionData.setDscMaxTiltAvg(0.0);
                    velocityAndDirectionData.setDcsStdTiltAvg(0.0);
                    break;
                case 16:
                    velocityAndDirectionData.setDcsModel(Integer.parseInt(parts[2]));
                    velocityAndDirectionData.setDcsSerial(Integer.parseInt(parts[3]));
                    velocityAndDirectionData.setDcsAbsspdAvg(Double.parseDouble(parts[4]));
                    velocityAndDirectionData.setDcsDirectionAvg(Double.parseDouble(parts[5]));
                    velocityAndDirectionData.setDcsNorthCurAvg(Double.parseDouble(parts[6]));
                    velocityAndDirectionData.setDcsEastCurAvg(Double.parseDouble(parts[7]));
                    velocityAndDirectionData.setDcsHeadingAvg(Double.parseDouble(parts[8]));
                    velocityAndDirectionData.setDcsTiltXAvg(Double.parseDouble(parts[9]));
                    velocityAndDirectionData.setDcsTiltYAvg(Double.parseDouble(parts[10]));
                    velocityAndDirectionData.setDcsSpStdAvg(Double.parseDouble(parts[11]));
                    velocityAndDirectionData.setDcsSigStrengthAvg(Double.parseDouble(parts[12]));
                    velocityAndDirectionData.setDcsPingCntAvg(Double.parseDouble(parts[13]));
                    velocityAndDirectionData.setDcsAbsTiltAvg(Double.parseDouble(parts[14]));
                    velocityAndDirectionData.setDscMaxTiltAvg(Double.parseDouble(parts[15]));
                    velocityAndDirectionData.setDcsStdTiltAvg(0.0);
                    break;
                default:
                    break;
            }
        }

        recordList += velocityAndDirectionData.getRecord();
        dcsModList += velocityAndDirectionData.getDcsModel();
        dcsSerialList += velocityAndDirectionData.getDcsSerial();
        dcsAbsspdAvgList += velocityAndDirectionData.getDcsAbsspdAvg();
        dcsDirectionAvgList += velocityAndDirectionData.getDcsDirectionAvg();
        dcsNorthCurAvgList += velocityAndDirectionData.getDcsNorthCurAvg();
        dcsEastCurAvgList += velocityAndDirectionData.getDcsEastCurAvg();
        dcsHeadingAvgList += velocityAndDirectionData.getDcsHeadingAvg();
        dcsTiltXAvgList += velocityAndDirectionData.getDcsTiltXAvg();
        dcsTiltYAvgList += velocityAndDirectionData.getDcsTiltYAvg();
        dcsSpStdAvgList += velocityAndDirectionData.getDcsSpStdAvg();
        dcsSigStrengthAvgList += velocityAndDirectionData.getDcsSigStrengthAvg();
        dcsPingCntAvgList += velocityAndDirectionData.getDcsPingCntAvg();
        dcsAbsTiltAvgList += velocityAndDirectionData.getDcsAbsTiltAvg();
        dscMaxTiltAvgList += velocityAndDirectionData.getDscMaxTiltAvg();
        dcsStdTiltAvgList += velocityAndDirectionData.getDcsStdTiltAvg();

        return velocityAndDirectionData;
    }

    /**
     * Takes the given ordered list of VelocityAndDirectionData properties and initialises a new
     * VelocityAndDirectionData object.
     * @param line - row to parse
     * @return VelocityAndDirectionData
     */
    public static VelocityAndDirectionData parseLine(List<String> line) {

        if (line.size() != 17) {
            throw new IllegalArgumentException("VelocityAndDirectionData Objects require 17 input arguments.");
        }

        return new VelocityAndDirectionData(
                Timestamp.valueOf(line.get(0)),
                Integer.parseInt(line.get(1)),
                Integer.parseInt(line.get(2)),
                Integer.parseInt(line.get(3)),
                Double.parseDouble(line.get(4)),
                Double.parseDouble(line.get(5)),
                Double.parseDouble(line.get(6)),
                Double.parseDouble(line.get(7)),
                Double.parseDouble(line.get(8)),
                Double.parseDouble(line.get(9)),
                Double.parseDouble(line.get(10)),
                Double.parseDouble(line.get(11)),
                Double.parseDouble(line.get(12)),
                Double.parseDouble(line.get(13)),
                Double.parseDouble(line.get(14)),
                Double.parseDouble(line.get(15)),
                Double.parseDouble(line.get(16))
        );
    }

    public static List<VelocityAndDirectionData> parseLines(List<String> lines) {
        var parsedLines = new ArrayList<VelocityAndDirectionData>();

        int count = 0;
        for (String line : lines) {
            if (count++ == 0) {
                // Skip header
                continue;
            }
            parsedLines.add(VelocityAndDirectionService.parseLine(line));
        }
        count--;
        recordList /= count;
        dcsModList /= count;
        dcsSerialList /= count;
        dcsAbsspdAvgList /= count;
        dcsDirectionAvgList /= count;
        dcsNorthCurAvgList /= count;
        dcsEastCurAvgList /= count;
        dcsHeadingAvgList /= count;
        dcsTiltXAvgList /= count;
        dcsTiltYAvgList /= count;
        dcsSpStdAvgList /= count;
        dcsSigStrengthAvgList /= count;
        dcsPingCntAvgList /= count;
        dcsAbsTiltAvgList /= count;
        dscMaxTiltAvgList /= count;
        dcsStdTiltAvgList /= count;

        VelocityAndDirectionData totalAverages = new VelocityAndDirectionData();
        totalAverages.setRecord(recordList);
        totalAverages.setDcsModel(dcsModList);
        totalAverages.setDcsSerial(dcsSerialList);
        totalAverages.setDcsAbsspdAvg(dcsAbsspdAvgList);
        totalAverages.setDcsDirectionAvg(dcsDirectionAvgList);
        totalAverages.setDcsNorthCurAvg(dcsNorthCurAvgList);
        totalAverages.setDcsEastCurAvg(dcsEastCurAvgList);
        totalAverages.setDcsHeadingAvg(dcsHeadingAvgList);
        totalAverages.setDcsTiltXAvg(dcsTiltXAvgList);
        totalAverages.setDcsTiltYAvg(dcsTiltYAvgList);
        totalAverages.setDcsSpStdAvg(dcsSpStdAvgList);
        totalAverages.setDcsSigStrengthAvg(dcsSigStrengthAvgList);
        totalAverages.setDcsPingCntAvg(dcsPingCntAvgList);
        totalAverages.setDcsAbsTiltAvg(dcsAbsTiltAvgList);
        totalAverages.setDscMaxTiltAvg(dscMaxTiltAvgList);
        totalAverages.setDcsStdTiltAvg(dcsStdTiltAvgList);

        parsedLines.add(totalAverages);

        return parsedLines;
    }


    public static String summarise(List<VelocityAndDirectionData> velocityAndDirectionDataset) {
        Integer totalLines = velocityAndDirectionDataset.size();
        VelocityAndDirectionData averageRecord = velocityAndDirectionDataset.get(totalLines-1);

        var summaryBuilder = new StringBuilder() ;

        // Transform dataset to be listed in columns rather than rows

        summaryBuilder.append("<head></head>")
                .append("<body>")
                .append("<h2>Summary</h2>")
                .append("<br />")
                .append("<strong>Total Lines:</strong>" + totalLines.toString())
                .append("<p></p>")
                .append("<p><strong>Average Record: </strong>" + averageRecord.getRecord() + "</p>")
                .append("<p><strong>Average dcsModel: </strong>" + averageRecord.getDcsModel() + "</p>")
                .append("<p><strong>Average dcsSerialList: </strong>" + averageRecord.getDcsSerial()+ "</p>")
                .append("<p><strong>Average dcsAbsspdAvgList: </strong>" + averageRecord.getDcsAbsspdAvg()+ "</p>")
                .append("<p><strong>Average dcsDirectionAvgList: </strong>" + averageRecord.getDcsDirectionAvg()+ "</p>")
                .append("<p><strong>Average dcsNorthCurAvgList: </strong>" + averageRecord.getDcsNorthCurAvg()+ "</p>")
                .append("<p><strong>Average dcsEastCurAvgList: </strong>" + averageRecord.getDcsEastCurAvg()+ "</p>")
                .append("<p><strong>Average dcsHeadingAvgList: </strong>" + averageRecord.getDcsHeadingAvg()+ "</p>")
                .append("<p><strong>Average dcsTiltXAvgList: </strong>" + averageRecord.getDcsTiltXAvg()+ "</p>")
                .append("<p><strong>Average dcsTiltYAvgList: </strong>" + averageRecord.getDcsTiltYAvg()+ "</p>")
                .append("<p><strong>Average dcsSpStdAvgList: </strong>" + averageRecord.getDcsSpStdAvg()+ "</p>")
                .append("<p><strong>Average dcsSigStrengthAvgList: </strong>" + averageRecord.getDcsSigStrengthAvg()+ "</p>")
                .append("<p><strong>Average dcsPingCntAvgList: </strong>" + averageRecord.getDcsPingCntAvg()+ "</p>")
                .append("<p><strong>Average dcsAbsTiltAvgList: </strong>" + averageRecord.getDcsAbsTiltAvg()+ "</p>")
                .append("<p><strong>Average dscMaxTiltAvgList: </strong>" + averageRecord.getDscMaxTiltAvg()+ "</p>")
                .append("<p><strong>Average dcsStdTiltAvgList: </strong>" + averageRecord.getDcsStdTiltAvg());  // Add p element to HTML for displaying averages

        // TODO: You will also have to do some work here to ensure the details are complete.
        summaryBuilder.append("</p>")
                .append("</body>");

        return summaryBuilder.toString();
    }
}
