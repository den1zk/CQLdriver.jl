module CQLdriver
export cqlinit, cqlclose, cqlwrite, cqlread

using DataFrames
include("cqlwrapper.jl")
const CQL_OK = 0x0000

function Base.size(result::Ptr{CassResult})
    rows = cql_result_row_count(result)   
    cols = cql_result_column_count(result)
    return (Int(rows)::Int, Int(cols)::Int)
end

"""
    function cql_future_check(future, caller)
Check if a future contains any errors
# Arguments
- `future::Ptr{CassFuture}`: a pointer to a future
- `caller::String`: a string to help identify where this function is called from
# Return
- `err::UInt`: a 16 bit integer with an error code. No error returns 0
"""
function cqlfuturecheck(future::Ptr{CassFuture}, caller::String = "")
    err = cql_future_error_code(future)
    # only prints valid messages for client errors
    if err != CQL_OK
        println("Error in CQL operation: ", caller)
        str = zeros(Vector{UInt8}(256))
        strref = Ref{Ptr{UInt8}}(pointer(str))
        siz = pointer_from_objref(sizeof(str))
        cql_future_error_message(future, strref, siz)
        println(unsafe_string(strref[]))
    end
    return err::UInt16
end

"""
    cql_val_type(result, idx)
Takes a CassResult and returns the type in a given column
# Arguments
- `result::Ptr{CassResult}`: a valid result from a query
- `idx::Int64`: the column to check
# Return
- `typ::DataType`: the type of the value in the specified column
"""
function cqlvaltype(result::Ptr{CassResult}, idx::Int64) 
# http://datastax.github.io/cpp-driver/api/cassandra.h/#enum-CassValueType
    val = cql_result_column_type(result, idx)
    val == 0x0001 ? typ = String   : # ASCII
    val == 0x000A ? typ = String   : # TEXT
    val == 0x0010 ? typ = String   : # INET
    val == 0x0011 ? typ = String   : # DATE
    val == 0x0012 ? typ = String   : # TIME
    val == 0x000D ? typ = String   : # VARCHAR
    val == 0x0014 ? typ = UInt8    : # TINYINT
    val == 0x0013 ? typ = UInt16   : # SMALLINT
    val == 0x000C ? typ = UInt128  : # UUID
    val == 0x000F ? typ = UInt128  : # TIMEUUID
    val == 0x0009 ? typ = Int32    : # INTEGER
    val == 0x0002 ? typ = Int64    : # BIGINT
    val == 0x0005 ? typ = Int64    : # COUNTER
    val == 0x000E ? typ = BigInt   : # VARINT
    val == 0x0004 ? typ = Bool     : # BOOLEAN
    val == 0x0007 ? typ = Float64  : # DOUBLE
    val == 0x0008 ? typ = Float32  : # FLOAT
    val == 0x0006 ? typ = BigFloat : # DECIMAL
    val == 0x000B ? typ = DateTime : # TIMESTAMP
    val == 0x0003 ? typ = Any      : # BLOB
    val == 0xFFFF ? typ = Any      : # UNKNOWN
    val == 0x0000 ? typ = Any      : # CUSTOM
    val == 0x0015 ? typ = Any      : # DURATION
    val == 0x0020 ? typ = Any      : # LIST
    val == 0x0021 ? typ = Any      : # MAP
    val == 0x0022 ? typ = Any      : # SET
    val == 0x0030 ? typ = Any      : # UDT
    val == 0x0031 ? typ = Any      : # TUPLE
    typ = Any
    return typ::DataType
end

"""
    function cqlgetvalue(val, t, strlen)
retrieve value using the correct type
# Arguments
- `val::Ptr{CassValue}`: a returned value from a query
- `t::DataType`: the type of the value being extracted
- `strlen::Int`: for string values specify max-length of output
# Return
- `out`: the return value, can by of any type
"""
function cqlgetvalue(val::Ptr{CassValue}, T::DataType, strlen::Int)
    if T == Int64
        num = Ref{Clonglong}(0)
        err = cql_value_get_int64(val, num)
        out = ifelse(err == CQL_OK, num[], NA)
        return out
    elseif T == Int32
        num = Ref{Cint}(0)
        err = cql_value_get_int32(val, num)
        out = ifelse(err == CQL_OK, num[], NA)
        return out
    elseif T == String
        str = zeros(Vector{UInt8}(strlen))
        strref = Ref{Ptr{UInt8}}(pointer(str))
        siz = pointer_from_objref(sizeof(str))
        err = cql_value_get_string(val, strref, siz)
        out = ifelse(err == CQL_OK, unsafe_string(strref[]), NA)
        return out
    elseif T == Float64
        num = Ref{Cdouble}(0)
        err = cql_value_get_double(val, num)
        out = ifelse(err == CQL_OK, num[], NA)
        return out
    elseif T == Float32
        num = Ref{Cfloat}(0)
        err = cql_value_get_float(val, num)
        out = ifelse(err == CQL_OK, num[], NA)
        return out
    elseif T == DateTime
        unixtime = Ref{Clonglong}(0)
        err = cql_value_get_int64(val, unixtime)
        out = ifelse(err == CQL_OK, Dates.unix2datetime(unixtime[]/1000), NA)
        return out
    end
    return NA
end

"""
    function cqlstrprep(table, data)
create a prepared query string for use with batch inserts
# Arguments
- `table::String`: name of the table on the server
- `columns::Array{String}`: name of the columns on the server
- `data::Array{Any,1}`: an array of data to be inserted
# Return
- `out::String`: a valid INSERT or UPDATE query
"""
function cqlstrprep(table::String, data::DataFrame; update::DataFrame=DataFrame(), counter::Bool=false)
    out = ""
    if isempty(update)
        datacolnames = string.(names(data))
        cols, vals = "", ""

        for c in datacolnames
            cols = cols * c * ","
            vals = vals * "?,"
        end
        out = "INSERT INTO " * table * " (" * cols[1:end-1] * ") VALUES (" * vals[1:end-1] * ")"
    else write == :update
        datacolnames = string.(names(data))
        updtcolnames = string.(names(update))
        cols, vals = "", ""
        for c in datacolnames
            
            cols = cols * c * "=" * ifelse(counter, c*"+?, ", "?, ")
        end
        for u in updtcolnames
            vals = vals * u * "=? AND "
        end
        out = "UPDATE " * table * " SET " * cols[1:end-2] * " WHERE " * vals[1:end-5]
    end
    return out::String
end

"""
    function cqlstatementbind(statement, pos, typ, data)
Bind data to a column in a statement for use with batch inserts
# Arguments
- `statement::Ptr{CassStatement}`: pointer to a statement
- `pos::Int`: what column to put data into
- `typ::DataType, data)`: the datatype of the data
# Return
- `Void`:
"""
function cqlstatementbind(statement::Ptr{CassStatement}, pos::Int, typ::DataType, data)
    if typ == String
        cql_statement_bind_string(statement, pos, data)
    elseif typ == Int32
        cql_statement_bind_int32(statement, pos, data)
    elseif typ == Int64
        cql_statement_bind_int64(statement, pos, data)
    elseif typ == Float32
        cql_statement_bind_float(statement, pos, data)
    elseif typ == Float64
        cql_statement_bind_double(statement, pos, data)
    elseif typ == DateTime
        d = convert(Int64, Dates.datetime2unix(data)*1000)
        cql_statement_bind_int64(statement, pos, d)
    end
end



"""
function cqlinit(hosts)    
Establish a new connection to a cluster
# Arguments
- `hosts::String`: a string of comma separated IP addresses
# Return
- `session::Ptr{CassSession}`: a pointer to the active session
- `cluster::Ptr{CassCluster}: a pointer to the active cluster`
- `err::UInt`: a 16 bit integer with an error code. No error returns 0
"""
function cqlinit(hosts::String)
    cluster = cql_cluster_new()
    session = cql_session_new()
    cql_cluster_set_contact_points(cluster, hosts)
    future = cql_session_connect(session, cluster)
    err = cqlfuturecheck(future, "Session Connect")
    cql_future_free(future)    
    return session::Ptr{CassSession}, cluster::Ptr{CassCluster}, err::UInt16
end

"""
function cqlclose(session, cluster)
Decommission a connection and free its resources
# Arguments
- `session::Ptr{CassSession}`: the current active session
- `cluster::Ptr{CassCluster}`: the cluster associated with the active session
# Return
- `Void`:
"""
function cqlclose(session::Ptr{CassSession}, cluster::Ptr{CassCluster})
    cql_session_free(session)
    cql_cluster_free(cluster)
end

"""
    function cqlread(session, query; pgsize, retries, strlen)
Query the server for the contents of a table
- `session::Ptr{CassSession}`: pointer to the active session
- `query::String`: a valid SELECT query
- `pgsize::Int=10000`: how many lines to pull at a time
- `retries::Int=5`: number of times to retry pulling a page of data
- `strlen::Int=128`: the maximum number of characters in a string
# Return
- `err::UInt16`: status of the query
- `output::DataFrame`: a dataframe with named columns
"""
function cqlread(session::Ptr{CassSession}, query::String; pgsize::Int=10000, retries::Int=5, strlen::Int=128)
    statement = cql_statement_new(query, 0)
    cql_statement_set_paging_size(statement, pgsize)
    
    output = DataFrame()
    morepages = true
    firstpage = true
    err = CQL_OK
    while(morepages)
        future = Ptr{CassFuture}
        while(true)
            future = cql_session_execute(session, statement)
            err = cqlfuturecheck(future, "Session Execute")
            err == CQL_OK && break
            if err != CQL_OK & retries == 0
                cql_statement_free(statement)
                cql_future_free(future)
                return err::UInt16, output::DataFrame 
            end
            sleep(1)
            retries -= 1
            cql_future_free(future)
        end    
        
        result = cql_future_get_result(future)
        cql_future_free(future)
        rows, cols = size(result)

        if firstpage
            types = Array{DataType}(cols)
            for c in 1:cols
                types[c] = cqlvaltype(result, c-1)
            end
            names = Array{Symbol}(cols)
            for c in 1:cols
                str = zeros(Vector{UInt8}(strlen))
                strref = Ref{Ptr{UInt8}}(pointer(str))
                siz = pointer_from_objref(sizeof(str))
                errcol = cql_result_column_name(result, c-1, strref, siz)
                names[c] = Symbol(ifelse(errcol == CQL_OK, unsafe_string(strref[]), string("C",c)))
            end
            output = DataFrame(types, names, 0)
            firstpage = false
        end

        iterator = cql_iterator_from_result(result)
        arraybuf = Array{Any}(cols)
        for r in 1:rows
            cql_iterator_next(iterator)
            row = cql_iterator_get_row(iterator)
            for c in 1:cols
                val = cql_row_get_column(row, c-1)
                arraybuf[c] = cqlgetvalue(val, types[c], strlen)
            end
            push!(output, arraybuf)     
        end
        
        morepages = cql_result_has_more_pages(result)
        cql_statement_set_paging_state(statement, result)
        cql_iterator_free(iterator)
        cql_result_free(result)
    end
    cql_statement_free(statement)
    return err::UInt16, output::DataFrame
end

"""
    function cqlbatchwrite(session, table, data; retries, update, counter)
Write a set of rows to a table as a prepared batch
# Arguments
- `session::Ptr{CassSession}`: pointer to the active session
- `table::String`: the name of the table you want to write to
- `data::DataFrame`: a DataFrame with named columns
- `retries::Int=5`: number of retries per batch insert
- `update::DataFrame`: the arguments for WHERE during an UPDATE
- `counter::Bool`: for updating the counter datatype
# Return
- `err::UInt16`: status of the batch insert
"""
function cqlbatchwrite(session::Ptr{CassSession}, table::String, data::DataFrame; retries::Int=5, update::DataFrame=DataFrame(), counter::Bool=false)
    query = cqlstrprep(table, data, update=update, counter=counter)
    future = cql_session_prepare(session, query)
    cql_future_wait(future)
    err = cqlfuturecheck(future, "Session Prepare") 
    if err != CQL_OK 
        cql_future_free(future)
        return err::UInt16
    end
    
    prep = cql_future_get_prepared(future)
    cql_future_free(future)
    batchtype = ifelse(!counter, 0x00, 0x02)
    batch = cql_batch_new(batchtype)
    rows, cols = size(data)
    frame = data
    if !isempty(size(update))
        urows, ucols = size(update)
        cols += ucols
        frame = hcat(data, update)
    end
    types = Array{DataType}(cols)
    for c in 1:cols
        types[c] = typeof(frame[1,c])
    end
    for r in 1:rows
        statement = cql_prepared_bind(prep)
        for c in 1:cols
            cqlstatementbind(statement, c-1, types[c], frame[r,c])
        end
        cql_batch_add_statement(batch, statement)
        cql_statement_free(statement)
    end
    while(true)
        future = cql_session_execute_batch(session, batch)
        cql_future_wait(future)
        err = cqlfuturecheck(future, "Execute Batch")
        cql_future_free(future)
        err == CQL_OK && break
        retries == 0 && break
        retries -= 1
        sleep(1)
    end
    cql_prepared_free(prep)
    cql_batch_free(batch)
    return err::UInt16
end

"""
    function cqlrowwrite(session, table, data; retries, update, counter)
Write one row of data to a table
# Arguments
- `session::Ptr{CassSession}`: pointer to the active session
- `table::String`: the name of the table you want to write to
- `data::DataFrame`: a DataFrame with named columns
- `retries::Int=5`: number of retries per batch insert
- `update::DataFrame`: the arguments for WHERE during an UPDATE
- `counter::Bool`: for updating the counter datatype
# Return
- `err::UInt16`: status of the insert
"""
function cqlrowwrite(session::Ptr{CassSession}, table::String, data::DataFrame; retries::Int=5, update::DataFrame=DataFrame(), counter::Bool=false)
    err = CQL_OK
    query = cqlstrprep(table, data, update=update, counter=counter)
    rows, cols = size(data)
    frame = data    
    if !isempty(size(update))
        urows, ucols = size(update)
        cols += ucols
        frame = hcat(data, update)
    end
    types = Array{DataType}(cols)
    for c in 1:cols
        types[c] = typeof(frame[1,c])
    end
    statement = cql_statement_new(query, cols)
    for c in 1:cols
        cqlstatementbind(statement, c-1, types[c], frame[1,c])
    end

    while(true) 
        future = cql_session_execute(session, statement)
        cql_future_wait(future)
        err = cqlfuturecheck(future, "Execute Statement")
        cql_future_free(future)
        err == CQL_OK && break
        retries == 0 && break
        retries -= 1
        sleep(1)
    end
    cql_statement_free(statement)
    return err::UInt16
end

"""
    function cqlwrite(session, table, data; batchsize, retries, update, counter)
Write to a table
# Arguments
- `session::Ptr{CassSession}`: pointer to the active session
- `table::String`: the name of the table you want to write to
- `data::DataFrame`: a DataFrame with named columns
- `retries::Int=5`: number of retries per batch insert
- `batchsize::Int=1000`: number of rows to write per batch
- `update::DataFrame`: the arguments for WHERE during an UPDATE
- `counter::Bool`: for updating the counter datatype
# Return
- `err::UInt16`: status of the insert
"""
function cqlwrite(s::Ptr{CassSession}, table::String, data::DataFrame; update::DataFrame=DataFrame(), batchsize::Int=1000, retries::Int=5, counter::Bool=false) 
    rows, cols = size(data)
    rows == 0 && return 0x9999
    if rows == 1
        err = cqlrowwrite(s, table, data, retries=retries, update=update, counter=counter)
    elseif rows <= batchsize
        err = cqlbatchwrite(s, table, data, retries=retries, update=update, counter=counter)
    else
        pages = (rows ÷ batchsize)
        err = zeros(Array{UInt16}(pages))
        @sync for p in 1:pages
            to = p * batchsize
            fr = to - batchsize + 1
            if p < pages
                @async err[p] = cqlbatchwrite(s, table, data[fr:to,:], retries=retries, update=update[fr:to,:], counter=counter)
            else
                @async err[p] = cqlbatchwrite(s, table, data[fr:end,:], retries=retries, update=update[fr:end,:], counter=counter)
            end
        end
        err = union(err)[1]
    end
    return err::UInt16
end

end