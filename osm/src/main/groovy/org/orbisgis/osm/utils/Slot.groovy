package org.orbisgis.osm.utils

import java.text.SimpleDateFormat

/**
 * Locked slot of the Overpass server which is free after a wait time.
 */
class Slot {

    /** String used to parse the slot {@link String} representation */
    private static final SLOT_AVAILABLE_AFTER = "Slot available after: "
    /** String used to parse the slot {@link String} representation */
    private static final IN = ", in "
    /** String used to parse the slot {@link String} representation */
    private static final SECONDS = " seconds."

    /** {@link SimpleDateFormat} used to parse dates. */
    private format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'", Locale.ENGLISH)
    private local = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'", Locale.FRANCE)

    /** {@link Date} when the slot will be available. */
    Date availibility
    /** Time in seconds to wait until the slot is available.*/
    long waitSeconds

    /**
     * Main constructor.
     * @param text {@link String} representation of the slot.
     */
    Slot(String text){
        format.setTimeZone(TimeZone.getTimeZone("Etc/GMT+0"))
        local.setTimeZone(TimeZone.getDefault())
        String[] values = (text - SLOT_AVAILABLE_AFTER - SECONDS).split(IN)
        availibility = format.parse(values[0])
        waitSeconds = Long.decode(values[1])
    }

    @Override
    String toString(){
        return "$SLOT_AVAILABLE_AFTER${local.format(availibility)}$IN$waitSeconds$SECONDS"
    }
}
