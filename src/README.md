

### Local build and deploy of the lambda function using the .env as context 
```cargo lambda watch --env-file .env```



### Invoke the function, passing in the file as a payload
```cargo lambda invoke gh-import-issues --data-file src/full_payload.json```


### Access local db
```psql -d "postgres://connorcampbell@localhost:5433/kudoslocal"```