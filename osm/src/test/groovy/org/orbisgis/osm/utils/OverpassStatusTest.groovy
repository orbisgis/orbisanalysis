package org.orbisgis.osm.utils


import org.junit.jupiter.api.Test
import org.orbisgis.osm.OSMTools

import java.text.SimpleDateFormat

import static org.junit.jupiter.api.Assertions.*

/**
 * Test class dedicated to the {@link OverpassStatus} class.
 *
 * @author Sylvain PALOMINOS (UBS LAB-STICC 2019)
 */
class OverpassStatusTest {
    private static final CONNECTION_ID = "3265774337"
    private static final CURRENT_TIME1 = "2019-10-11T13:31:36Z"
    private static final CURRENT_TIME2 = "2019-10-11T15:31:36Z"
    private static final RATE_LIMIT = 3
    private static final SLOT_AVAILABLE = 1
    private static final SLOT_AVAILABLE_AFTER_DATE1 = "2019-10-11T13:31:39Z"
    private static final SLOT_AVAILABLE_AFTER_TIME1 = "3"
    private static final SLOT_AVAILABLE_AFTER_DATE2 = "2019-10-11T15:31:39Z"
    private static final PID = "31941"
    private static final SPACE_LIMIT = "536870912"
    private static final TIME_LIMIT1 = "10"
    private static final TIME_LIMIT2 = "3"
    private static final START_TIME1 = "2019-10-11T13:31:35Z"
    private static final START_TIME2 = "2019-10-11T15:31:35Z"

    private format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'", Locale.ENGLISH)

    /**
     * Test the parsing of the {@link String} overpass server status representation.
     */
    @Test
    void overpassStatusTest(){
        def testStatus = "Connected as: $CONNECTION_ID\n" +
                "Current time: $CURRENT_TIME1\n" +
                "Rate limit: $RATE_LIMIT\n" +
                "$SLOT_AVAILABLE slots available now.\n" +
                "Slot available after: $SLOT_AVAILABLE_AFTER_DATE1, in $SLOT_AVAILABLE_AFTER_TIME1 seconds.\n" +
                "Currently running queries (pid, space limit, time limit, start time):\n" +
                "$PID\t$SPACE_LIMIT\t$TIME_LIMIT1\t$START_TIME1"
        def expected = "Connected as: $CONNECTION_ID\n" +
                "Current time: $CURRENT_TIME2\n" +
                "Rate limit: $RATE_LIMIT\n" +
                "$SLOT_AVAILABLE slots available now.\n" +
                "Slot available after: $SLOT_AVAILABLE_AFTER_DATE2, in $SLOT_AVAILABLE_AFTER_TIME1 seconds.\n" +
                "Currently running queries (pid, space limit, time limit, start time):\n" +
                "$PID\t$SPACE_LIMIT\t$TIME_LIMIT1\t$START_TIME2"
        def status = new OverpassStatus(testStatus)
        assertEquals expected.toString(), status.toString()

        assertEquals CONNECTION_ID, status.connectionId.toString()
        assertEquals CURRENT_TIME2, format.format(status.time)
        assertEquals RATE_LIMIT, status.slotLimit
        assertEquals SLOT_AVAILABLE, status.slotAvailable

        assertEquals 1, status.slots.size()
        Slot slot = status.slots[0]
        assertEquals format.parse(SLOT_AVAILABLE_AFTER_DATE2), slot.availibility
        assertEquals SLOT_AVAILABLE_AFTER_TIME1, slot.waitSeconds.toString()

        assertEquals 1, status.runningQueries.size()
        RunningQuery runningQuery = status.runningQueries[0]
        assertEquals PID, runningQuery.pid.toString()
        assertEquals SPACE_LIMIT, runningQuery.spaceLimit.toString()
        assertEquals TIME_LIMIT1, runningQuery.timeLimit.toString()
        assertEquals format.parse(START_TIME2), runningQuery.startTime
    }

    /**
     * Test the {@link OverpassStatus#wait(int)} method
     */
    @Test
    void waitTest(){
        def serverFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'", Locale.ENGLISH)
        serverFormat.setTimeZone(TimeZone.getTimeZone("Etc/GMT+0"))
        def current = Date.newInstance()
        def ds5 = 5
        def ds10 = 10
        def in5Sec = Date.from(current.toInstant().plusSeconds(ds5))
        def in10Sec = Date.from(current.toInstant().plusSeconds(ds10))
        long time1 = System.currentTimeSeconds()
        def slotBeforeQuery = "Connected as: $CONNECTION_ID\n" +
                "Current time: ${serverFormat.format(current)}\n" +
                "Rate limit: $RATE_LIMIT\n" +
                "0 slots available now.\n" +
                "Slot available after: ${serverFormat.format(in5Sec)}, in $ds5 seconds.\n" +
                "Currently running queries (pid, space limit, time limit, start time):\n" +
                "$PID\t$SPACE_LIMIT\t$TIME_LIMIT1\t${serverFormat.format(in10Sec)}"
        new OverpassStatus(slotBeforeQuery).waitForSlot(15)
        assertTrue ds5 <= System.currentTimeSeconds() - time1
        assertTrue ds10 > System.currentTimeSeconds() - time1


        current = Date.newInstance()
        def ds3 = 3
        def ds8 = 8
        def bef3Sec = Date.from(current.toInstant().minusSeconds(ds3))
        def in8Sec = Date.from(current.toInstant().plusSeconds(ds8))
        long time2 = System.currentTimeSeconds()
        def queryBeforeSlot = "Connected as: $CONNECTION_ID\n" +
                "Current time: ${serverFormat.format(current)}\n" +
                "Rate limit: $RATE_LIMIT\n" +
                "0 slots available now.\n" +
                "Slot available after: ${serverFormat.format(in8Sec)}, in $ds8 seconds.\n" +
                "Currently running queries (pid, space limit, time limit, start time):\n" +
                "$PID\t$SPACE_LIMIT\t$ds8\t${serverFormat.format(bef3Sec)}"
        new OverpassStatus(queryBeforeSlot).waitForQueryEnd(15)
        assertTrue ds8-ds3 <= System.currentTimeSeconds() - time2
        assertTrue ds8 > System.currentTimeSeconds() - time2
    }
}