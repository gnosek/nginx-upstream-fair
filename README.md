Nginx Upstream Fair Proxy Load Balancer
============

Description
-------

The Nginx fair proxy balancer enhances the standard round-robin load balancer provided
with Nginx so that it will track busy back end servers (e.g. Thin, Ebb, Mongrel)
and balance the load to non-busy server processes.

Installation
-------

You'll need to re-compile Nginx from source to include this module.
Modify your compile of Nginx by adding the following directive
(modified to suit your path of course):

```bash
./configure --with-http_ssl_module --add-module=/absolute/path/to/nginx-upstream-fair
make
make install
```


Configuration
-------
*Example*

    upstream mongrel {
        fair;
        server server1;
        server server2;
        server server3;
    }

The module supports a number of configuration directives:
* syntax: **fair**

  context: upstream

  Enables fairness.


* syntax: **upstream_fair_shm_size \<size\>;**
  
  context: main

  default: upstream_fair_shm_size 32k

  Size of the shared memory for storing information about the busy-ness of backends. Defaults to 8 pages
  (so 32k on most systems).

Usage
-------

Change your Nginx config file's upstream block to include the 'fair' directive:


If you encounter any issues, please report them using the [bugtracker](https://github.com/gnosek/nginx-upstream-fair/issues)

Contributing
-------

Git [source repository](http://github.com/gnosek/nginx-upstream-fair/tree/master)

Please feel free to fork the project at GitHub and submit pull requests or patches.

