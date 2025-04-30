package it.polimi.ds.reliable_queuing_system.utils;

import com.google.gson.*;

import java.lang.reflect.Type;

public class AddressJsonAdapter implements JsonSerializer<Address>, JsonDeserializer<Address> {

    @Override
    public Address deserialize(JsonElement jsonElement, Type type, JsonDeserializationContext jsonDeserializationContext) throws JsonParseException {
        String[] parts = jsonElement.getAsString().split(":");
        return new Address(parts[0], Integer.parseInt(parts[1]));
    }

    @Override
    public JsonElement serialize(Address address, Type type, JsonSerializationContext jsonSerializationContext) {
        return new JsonPrimitive(address.ip() + ":" + address.port());
    }
}
