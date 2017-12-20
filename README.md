# safe-distributor

A NiFi processor created for distributing messages between relationships.
The distribution will always transfer flowfiles with the same selected attribute value to the same relationship.

## Compile

`mvn clean install`

Copy the created nar to your NiFi nar lib.

```bash
cp ./nifi-safe-distributor-nar/target/nifi-safe-distributor-nar-1.0.nar `$NIFI_HOME/lib/`
```

Then restart nifi

```bash
sudo service nifi restart
```

## Testing

`mvn test`

## License

Apache 2.0