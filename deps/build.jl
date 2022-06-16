using Libdl
version = "2.11.0"
if Sys.islinux()
    const has_driver = !isempty(Libdl.find_library(["libcassandra"]))
    const has_yum = try success(`yum --version`) catch e false end
    const has_apt = try success(`apt-get -v`) && success(`apt-cache -v`) catch e false end

    if has_driver
        println("Cassandra CPP driver already installed.")
    elseif has_yum
        cass_url = "http://downloads.datastax.com/cpp-driver/centos/7/cassandra/v" * version * "/"
        cass_file = "cassandra-cpp-driver-" * version * "-1.el7.centos.x86_64.rpm"
        cass_source = cass_url * cass_file
        cass_target = "/tmp/cassandra-cpp-driver.rpm"
        dl = try success(`wget -O $cass_target $cass_source`) catch e false end
        !dl && error("Unable to download CPP driver.")
        inst = try success(`sudo yum install -y $cass_target`) catch e false end
        !inst && error("Unable to install CPP driver.")
    elseif has_apt
        ubuntu_version = chomp(read(pipeline(`cat /etc/os-release`, `grep -Eo "VERSION_ID=\"[0-9\.]+\""`, `grep -Eo "[^\"]+"`, `grep -E "[0-9.]+"`), String))
        cass_url = "http://downloads.datastax.com/cpp-driver/ubuntu/$(ubuntu_version)/cassandra/v" * version * "/"
        cass_file = "cassandra-cpp-driver_" * version * "-1_amd64.deb"
        cass_source = cass_url * cass_file
        libuv_url = "http://downloads.datastax.com/cpp-driver/ubuntu/$(ubuntu_version)/dependencies/libuv/v1.23.0/libuv1_1.23.0-1_amd64.deb"
        cass_target = "/tmp/cassandra-cpp-driver.deb"
        libuv_target = "/tmp/libuv.deb"
        libuv_dl = success(`wget -O $libuv_target $libuv_url`)
        !libuv_dl && error("Unable to download libuv.")
        libuv_inst = success(`sudo dpkg -i $libuv_target`)
        !libuv_inst && error("Unable to install libuv driver.")
        dl = try success(`wget -O $cass_target $cass_source`) catch e false end
        !dl && error("Unable to download CPP driver.")
        inst = try success(`sudo dpkg -i $cass_target`) catch e false end
        !inst && error("Unable to install CPP driver.")
        command = `sed -i '' -e 's/CASSLIBNAME/libcassandra.so.2/g' ../src/cqlwrapper.jl`
        sedresult = try run(command) catch e false end
        sedresult == false && error("could not build!")
    else
        error("This package requires cassandra-cpp-driver to be installed, but the build system only understands apt and yum.")
    end
end

if Sys.isapple()
    hascassandra = isfile("/usr/local/lib/libcassandra.dylib")
    hascrypt = isfile("/usr/local/opt/openssl/lib/libcrypto.3.dylib") || isfile("/usr/local/opt/openssl/lib/libcrypto.1.1.dylib")
    hasssl = isfile("/usr/local/opt/openssl/lib/libssl.3.dylib") || isfile("/usr/local/opt/openssl/lib/libssl.1.1.dylib")
    if !hascassandra
        command = `brew install cassandra-cpp-driver`
        cassandraresult = try run(command) catch e false end
    end    
    if !hascrypt || !hasssl
        command = `brew install openssl`
        sslresult = try run(command) catch e false end
    end    
    if !hascrypt || !hasssl || !hascassandra
        error("libcassandra and libcrypto and libssl must exist!")
    end    
    if !isfile("/usr/local/opt/openssl/lib/libcrypto.1.1.dylib") 
        cp("/usr/local/opt/openssl/lib/libcrypto.3.dylib", "/usr/local/opt/openssl/lib/libcrypto.1.1.dylib")
    end
    if !isfile("/usr/local/opt/openssl/lib/libssl.1.1.dylib") 
        cp("/usr/local/opt/openssl/lib/libssl.3.dylib", "/usr/local/opt/openssl/lib/libssl.1.1.dylib")
    end

    command = `sed -i '' -e 's/CASSLIBNAME/libcassandra.dylib/g' ../src/cqlwrapper.jl`
    sedresult = try run(command) catch e false end
    sedresult == false && error("could not build!")
end
