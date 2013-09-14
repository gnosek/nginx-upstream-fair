# Nginx Upstream Fair Proxy Load Balancer

## Description:

The Nginx fair proxy balancer enhances the standard round-robin load balancer provided
with Nginx so that it will track busy back end servers (e.g. Thin, Ebb, Mongrel)
and balance the load to non-busy server processes.

Further information can be found on http://nginx.localdomain.pl/

Ezra Zygmuntowicz has a good writeup of the fair proxy load balancer and how to use it here:
http://brainspl.at/articles/2007/11/09/a-fair-proxy-balancer-for-nginx-and-mongrel


## Installation:

You'll need to re-compile Nginx from source to include this module.
Modify your compile of Nginx by adding the following directive
(modified to suit your path of course):

```bash
./configure --with-http_ssl_module --add-module=/absolute/path/to/nginx-upstream-fair
make
make install
```


## Usage:
--

Change your Nginx config file's upstream block to include the 'fair' directive:

```nginx
upstream mongrel {
    fair;
    server 127.0.0.1:5000;
    server 127.0.0.1:5001;
    server 127.0.0.1:5002;
  }
```

If you encounter any issues, please report them using the bugtracker at
http://nginx.localdomain.pl/

## Contributing:
--

Git source repositories:
http://github.com/gnosek/nginx-upstream-fair/tree/master
http://git.localdomain.pl/?p=nginx-upstream-fair.git;a=summary

Please feel free to fork the project at GitHub and submit pull requests or patches.

