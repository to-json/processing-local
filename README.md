# processing-local

You probably don't need this, it's just auto-reload boilerplate for p5.js
so that my kid can more easily do Nature of Code.

`go build` builds reload-server. run that to serve index.html, which
mounts sketch.js. the server sends sse on local file changes, so
you could hypothetically do something cooler than reloading, but
for now, reloading is what we need.

upl covers server, index, and sketch, the vendored p5 is of course
still lgpl. vendored for load time, but i did test with cdn-served
and that worked fine if you're into that sort of thing
