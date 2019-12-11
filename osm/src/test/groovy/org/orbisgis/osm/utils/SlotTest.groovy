package org.orbisgis.osm.utils

import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.orbisgis.osm.Utilities

import java.text.SimpleDateFormat
import java.time.ZoneId

import static org.junit.jupiter.api.Assertions.*

/**
 * Test class for {@link Utilities}
 *
 * @author Sylvain PALOMINOS (UBS LAB-STICC 2019)
 */
class SlotTest {

    private static format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'")
    private static local = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'")

    @BeforeAll
    static void beforeAll(){
        format.setTimeZone(TimeZone.getTimeZone("Etc/GMT+0"))
        local.setTimeZone(TimeZone.getTimeZone(ZoneId.systemDefault()))
    }

    /**
     * Test the {@link Slot#Slot(java.lang.String)} method.
     */
    @Test
    void slotTest(){
        def date = "2019-10-11T13:31:39Z"
        def slot = new Slot("Slot available after: $date, in 3 seconds.")
        assertEquals 3, slot.waitSeconds
        assertEquals format.parse(date), slot.availibility
        slot = new Slot("Slot available after: $date, in 3 seconds.".toString())
        assertEquals 3, slot.waitSeconds
        assertEquals format.parse(date), slot.availibility
    }

    /**
     * Test the {@link Slot#toString()} method.
     */
    @Test
    void toStringTest(){
        def date = "2019-10-11T13:31:39Z"
        def slot = new Slot("Slot available after: $date, in 3 seconds.")
        assertEquals "Slot available after: ${local.format(format.parse(date))}, in 3 seconds.".toString(), slot.toString()
    }
}
