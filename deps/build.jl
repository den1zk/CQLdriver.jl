using Libdl
using CpuId
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
    isarm = try cpuinfo() catch e true end
    cassloc = isarm === true ? "/opt/homebrew/lib/libcassandra.dylib" : "/usr/local/lib/libcassandra.dylib"
    sslloc = isarm === true ? "/opt/homebrew/lib/libssl.3.dylib" : "/usr/local/opt/openssl/lib/libssl.3.dylib"
    cryptoloc = isarm === true ? "/opt/homebrew/lib/libcrypto.3.dylib" : "/usr/local/opt/openssl/lib/libcrypto.3.dylib"
    ssltransferloc = isarm === true ?  "/usr/local/lib/libssl.1.1.dylib" : "/usr/local/opt/openssl/lib/libssl.1.1.dylib"
    cryptotransferloc = isarm === true ?  "/usr/local/lib/libcrypto.1.1.dylib" : "/usr/local/opt/openssl/lib/libcrypto.1.1.dylib"
    if !isfile(cassloc)
        command = isarm === true ?  `arch -arm64 brew install cassandra-cpp-driver` : `brew install cassandra-cpp-driver`
        isarm === true && run(`cp /opt/homebrew/lib/libcassandra.dylib /usr/local/lib/libcassandra.dylib`)
        cassandraresult = try run(command) catch e false end
    end    
    if !(isfile(cryptoloc)) || 
        !(isfile(sslloc))
        command = `brew install openssl`
        sslresult = run(command)
    end
    hascassandra = isfile(cassloc)
    hascrypt = isfile(cryptoloc)
    hasssl = isfile(sslloc)
    if !hascrypt || !hasssl || !hascassandra
        error("libcassandra and libcrypto and libssl must exist!")
    end    
    if !isfile(cryptotransferloc) && isarm !== true
        cp(cryptoloc, cryptotransferloc)
    end
    if !isfile(ssltransferloc) && isarm !== true
        cp(sslloc, ssltransferloc)
    end

    command = `sed -i '' -e 's/CASSLIBNAME/libcassandra.dylib/g' ../src/cqlwrapper.jl`
    sedresult = try run(command) catch e false end
    sedresult == false && error("could not build!")
end
