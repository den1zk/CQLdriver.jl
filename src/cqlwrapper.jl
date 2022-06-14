using Base.Libc

# Cassandra uses a different ordering of the 8 bytes of UUIDs than Julia does, so we need conversion utilities.
struct CassUuid
    underlying_bytes::NTuple{16,UInt8}
end
CassUuid(uuid::UUID) = CassUuid(reinterpret(NTuple{16, UInt8}, [uuid.value])[1][cat(13:16, 11:12, 9:10, 1:8, dims=1)])
UUID(cass_uuid::CassUuid) = UUID(reinterpret(UInt128, [cass_uuid.underlying_bytes[cat(9:16, 7:8, 5:6, 1:4, dims=1)]])[1])

Base.convert(::Type{CassUuid}, uuid::UUID) = CassUuid(uuid)
Base.convert(::Type{UUID}, cass_uuid::CassUuid) = UUID(cass_uuid)

get_null_cass_uuid_ref()::Ref{CassUuid} = Ref(CassUuid(NTuple{16, UInt8}(0x00 for i in 1:16)))

macro genstruct(x)
    return :(mutable struct $x end)
end

@genstruct CassFuture
@genstruct CassCluster
@genstruct CassSession
@genstruct CassString
@genstruct CassStatement
@genstruct CassResult
@genstruct CassIterator
@genstruct CassRow
@genstruct CassValue
@genstruct CassPrepared
@genstruct CassBatch
@genstruct CassUuidGen

function cql_cluster_set_concurrency(cluster::Ptr{CassCluster}, nthreads::Int64)
    val = ccall(
            (:cass_cluster_set_num_threads_io, "CASSLIBNAME"),
            UInt16,
            (Ptr{CassCluster}, UInt32),
            cluster, nthreads)
    return val::UInt16
end

function cql_cluster_set_connections_per_host(cluster::Ptr{CassCluster}, siz::Int64)
    val1 = ccall(
        (:cass_cluster_set_max_connections_per_host, "CASSLIBNAME"),
        UInt16,
        (Ptr{CassCluster}, UInt32),
        cluster, siz)
    val2 = ccall(
        (:cass_cluster_set_core_connections_per_host, "CASSLIBNAME"),
        UInt16,
        (Ptr{CassCluster}, UInt32),
        cluster, siz)
    val = val1 | val2
    return val::UInt16
end

function cql_cluster_set_write_bytes_high_water_mark(cluster::Ptr{CassCluster}, siz::Int64)
    val = ccall(
        (:cass_cluster_set_write_bytes_high_water_mark, "CASSLIBNAME"),
        UInt16,
        (Ptr{CassCluster}, UInt32),
        cluster, siz)
    return val::UInt16
end

function cql_cluster_set_pending_requests_high_water_mark(cluster::Ptr{CassCluster}, siz::Int64)
    val = ccall(
        (:cass_cluster_set_pending_requests_high_water_mark, "CASSLIBNAME"),
        UInt16,
        (Ptr{CassCluster}, UInt32),
        cluster, siz)
    return val::UInt16
end

function cql_cluster_set_queue_size(cluster::Ptr{CassCluster}, siz::Int64)
    val1 = ccall(
        (:cass_cluster_set_queue_size_io, "CASSLIBNAME"),
        UInt16,
        (Ptr{CassCluster}, UInt32),
        cluster, siz)
    val2 = ccall(
        (:cass_cluster_set_queue_size_event, "CASSLIBNAME"),
        UInt16,
        (Ptr{CassCluster}, UInt32),
        cluster, siz)
    val = val1 | val2
    return val::UInt16
end

function cql_future_error_code(future::Ptr{CassFuture})
    val = ccall(
            (:cass_future_error_code, "CASSLIBNAME"),
            UInt16,
            (Ptr{CassFuture},),
            future)
    println("error code check", val)        
    return val::UInt16
end

function cql_future_error_message(future::Ptr{CassFuture}, strref::Ref{Ptr{UInt8}}, siz::Ref{Csize_t})
    ccall(
        (:cass_future_error_message, "CASSLIBNAME"),
        Nothing,
        (Ptr{CassFuture}, Ref{Ptr{UInt8}}, Ref{Csize_t}),
        future, strref, siz)
end

function cql_cluster_new()
    val = ccall(
            (:cass_cluster_new, "CASSLIBNAME"),
            Ptr{CassCluster},
            ())
    return val::Ptr{CassCluster}
end

function cql_session_new()
    val = ccall(
            (:cass_session_new, "CASSLIBNAME"),
            Ptr{CassSession},
            ())
    return val::Ptr{CassSession}
end

function cql_cluster_set_credentials(cluster::Ptr{CassCluster}, username::String, password::String)
    ccall(
        (:cass_cluster_set_credentials, "CASSLIBNAME"),
        Nothing,
        (Ptr{CassCluster}, Cstring, Cstring),
        cluster, username, password)
end

function cql_cluster_set_contact_points(cluster::Ptr{CassCluster}, hosts::String)
    ccall(
        (:cass_cluster_set_contact_points, "CASSLIBNAME"),
        Nothing,
        (Ptr{CassCluster}, Cstring),
        cluster, hosts)
end

function cql_cluster_set_whitelist_filtering(cluster::Ptr{CassCluster}, hosts::String)
    ccall(
        (:cass_cluster_set_whitelist_filtering, "CASSLIBNAME"),
        Nothing,
        (Ptr{CassCluster}, Cstring),
        cluster, hosts)
end

function cql_cluster_set_blacklist_filtering(cluster::Ptr{CassCluster}, hosts::String)
    ccall(
        (:cass_cluster_set_blacklist_filtering, "CASSLIBNAME"),
        Nothing,
        (Ptr{CassCluster}, Cstring),
        cluster, hosts)
end

function cql_session_connect(session::Ptr{CassSession}, cluster::Ptr{CassCluster})
    val = ccall(
            (:cass_session_connect, "CASSLIBNAME"),
            Ptr{CassFuture},
            (Ptr{CassSession}, Ptr{CassCluster}),
            session, cluster)
    return val::Ptr{CassFuture}
end

function cql_session_free(session::Ptr{CassSession})
    ccall(
        (:cass_session_free, "CASSLIBNAME"),
        Nothing,
        (Ptr{CassCluster},),
        session)
end

function cql_cluster_free(cluster::Ptr{CassCluster})
    ccall(
        (:cass_cluster_free, "CASSLIBNAME"),
        Nothing,
        (Ptr{CassCluster},),
        cluster)
end

function cql_result_row_count(result::Ptr{CassResult})
    val = ccall(
            (:cass_result_row_count, "CASSLIBNAME"),
            Int32,
            (Ptr{CassResult},),
            result)
    return val::Int32
end

function cql_result_column_count(result::Ptr{CassResult})
    val = ccall(
            (:cass_result_column_count, "CASSLIBNAME"),
            Int32,
            (Ptr{CassResult},),
            result)
    return val::Int32
end

function cql_iterator_next(iterator::Ptr{CassIterator})
    next = ccall(
            (:cass_iterator_next, "CASSLIBNAME"),
            UInt8,
            (Ptr{CassIterator},),
            iterator)
    val = ifelse(next == 0, false, true)
    return val::Bool
end

function cql_future_free(future::Ptr{CassFuture})
    ccall(
        (:cass_future_free, "CASSLIBNAME"),
        Nothing,
        (Ptr{CassFuture},),
        future)
end

function cql_result_column_type(result::Ptr{CassResult}, idx::Int64)
    val = ccall(
            (:cass_result_column_type, "CASSLIBNAME"),
            UInt16,
            (Ptr{CassResult}, UInt32),
            result, idx)
    return val::UInt16
end

function cql_value_get_uuid(val::Ptr{CassValue}, out::Ref{CassUuid})
    err = ccall(
            (:cass_value_get_uuid, "CASSLIBNAME"),
            Cushort,
            (Ptr{CassValue}, Ref{CassUuid}),
            val, out)
    return err::UInt16
end

function cql_value_get_int8(val::Ptr{CassValue}, out::Ref{Cshort})
    err = ccall(
            (:cass_value_get_int8, "CASSLIBNAME"),
            Cushort,
            (Ptr{CassValue}, Ref{Cshort}),
            val, out)
    return err::UInt16
end

function cql_value_get_int16(val::Ptr{CassValue}, out::Ref{Cshort})
    err = ccall(
            (:cass_value_get_int16, "CASSLIBNAME"),
            Cushort,
            (Ptr{CassValue}, Ref{Cshort}),
            val, out)
    return err::UInt16
end

function cql_value_get_int64(val::Ptr{CassValue}, out::Ref{Clonglong})
    err = ccall(
            (:cass_value_get_int64, "CASSLIBNAME"),
            Cushort,
            (Ptr{CassValue}, Ref{Clonglong}),
            val, out)
    return err::UInt16
end

function cql_value_get_int32(val::Ptr{CassValue}, out::Ref{Cint})
    err = ccall(
            (:cass_value_get_int32, "CASSLIBNAME"),
            Cushort,
            (Ptr{CassValue}, Ref{Cint}),
            val, out)
    return err::UInt16
end

function cql_result_column_name(val::Ptr{CassResult}, pos::Int, out::Ref{Ptr{UInt8}}, siz::Ref{Csize_t})
    err = ccall(
            (:cass_result_column_name, "CASSLIBNAME"),
            Cushort,
            (Ptr{CassResult}, Clonglong, Ref{Ptr{UInt8}}, Ref{Csize_t}),
            val, pos, out, siz)
    return err::UInt16
end

function cql_value_get_uint32(val::Ptr{CassValue}, out::Ref{Cuint})
    err = ccall(
            (:cass_value_get_uint32, "CASSLIBNAME"),
            Cushort,
            (Ptr{CassValue}, Ref{Cuint}),
            val, out)
    return err::UInt16
end

function cql_value_get_bool(val::Ptr{CassValue}, out::Ref{Cint})
    err = ccall(
            (:cass_value_get_bool, "CASSLIBNAME"),
            Cushort,
            (Ptr{CassValue}, Ref{Cint}),
            val, out)
    return err::UInt16
end

function cql_value_get_string(val::Ptr{CassValue}, out::Ref{Ptr{UInt8}}, siz::Ref{Csize_t})
    err = ccall(
            (:cass_value_get_string, "CASSLIBNAME"),
            Cushort,
            (Ptr{CassValue}, Ref{Ptr{UInt8}}, Ref{Csize_t}),
            val, out, siz)
    return err::UInt16
end

function cql_value_get_float(val::Ptr{CassValue}, out::Ref{Cfloat})
    err = ccall(
            (:cass_value_get_float, "CASSLIBNAME"),
            Cushort,
            (Ptr{CassValue}, Ref{Cfloat}),
            val, out)
    return err::UInt16
end

function cql_value_get_double(val::Ptr{CassValue}, out::Ref{Cdouble})
    err = ccall(
            (:cass_value_get_double, "CASSLIBNAME"),
            Cushort,
            (Ptr{CassValue}, Ref{Cdouble}),
            val, out)
    return err::UInt16
end

function cql_statement_free(statement::Ptr{CassStatement})
    ccall(
        (:cass_statement_free, "CASSLIBNAME"),
        Nothing,
        (Ptr{CassStatement},),
        statement)
end

function cql_result_free(result::Ptr{CassResult})
    ccall(
        (:cass_result_free, "CASSLIBNAME"),
        Nothing,
        (Ptr{CassResult},),
        result)
end

function cql_iterator_free(iterator::Ptr{CassIterator})
    ccall(
        (:cass_iterator_free, "CASSLIBNAME"),
        Nothing,
        (Ptr{CassIterator},),
        iterator)
end

function cql_statement_new(query::String, params::Int)
    statement = ccall(
                    (:cass_statement_new, "CASSLIBNAME"),
                    Ptr{CassStatement},
                    (Cstring, Clonglong),
                    query, params)
    return statement::Ptr{CassStatement}
end

function cql_statement_set_paging_size(statement::Ptr{CassStatement}, pgsize::Int)
    ccall(
        (:cass_statement_set_paging_size, "CASSLIBNAME"),
        Nothing,
        (Ptr{CassStatement}, Cint),
        statement, pgsize)
end

function cql_statement_set_request_timeout(statement::Ptr{CassStatement}, timeout::Int)
    err = ccall(
            (:cass_cluster_set_request_timeout, "CASSLIBNAME"),
            Cushort,
            (Ptr{CassStatement}, Clonglong),
            statement, timeout)
    return err::UInt16
end

function cql_session_execute(session::Ptr{CassSession}, statement::Ptr{CassStatement})
    future = ccall(
                (:cass_session_execute, "CASSLIBNAME"),
                Ptr{CassFuture},
                (Ptr{CassSession}, Ptr{CassStatement}),
                session, statement)
    println("buradayim..")            
    return future::Ptr{CassFuture}
end

function cql_future_get_result(future::Ptr{CassFuture})
    result = ccall(
                (:cass_future_get_result, "CASSLIBNAME"),
                Ptr{CassResult},
                (Ptr{CassFuture},),
                future)
    return result::Ptr{CassResult}
end

function cql_iterator_from_result(result::Ptr{CassResult})
    iterator = ccall(
                (:cass_iterator_from_result, "CASSLIBNAME"),
                Ptr{CassIterator},
                (Ptr{CassResult},),
                result)
    return iterator::Ptr{CassIterator}
end

function cql_iterator_get_row(iterator::Ptr{CassIterator})
    row = ccall(
            (:cass_iterator_get_row, "CASSLIBNAME"),
            Ptr{CassRow},
            (Ptr{CassIterator},),
            iterator)
    return row::Ptr{CassRow}
end

function cql_row_get_column(row::Ptr{CassRow}, pos::Int64)
    val = ccall(
            (:cass_row_get_column, "CASSLIBNAME"),
            Ptr{CassValue},
            (Ptr{CassRow}, Clonglong),
            row, pos)
    return val::Ptr{CassValue}
end

function cql_result_has_more_pages(result::Ptr{CassResult})
    hasmore = ccall(
                (:cass_result_has_more_pages, "CASSLIBNAME"),
                Cint,
                (Ptr{CassResult},),
                result)
    out = convert(Bool, hasmore)
    return out::Bool
end

function cql_statement_set_paging_state(statement::Ptr{CassStatement}, result::Ptr{CassResult})
    ccall(
        (:cass_statement_set_paging_state, "CASSLIBNAME"),
        Nothing,
        (Ptr{CassStatement}, Ptr{CassResult}),
        statement, result)
end

function cql_future_wait(future::Ptr{CassFuture})
    ccall(
        (:cass_future_wait, "CASSLIBNAME"),
        Nothing,
        (Ptr{CassFuture},),
        future)
end

function cql_session_prepare(session::Ptr{CassSession}, query::String)
    future = ccall(
                (:cass_session_prepare, "CASSLIBNAME"),
                Ptr{CassFuture},
                (Ptr{CassSession}, Cstring),
                session, query)
    return future::Ptr{CassFuture}
end

function cql_batch_new(batch_type::UInt8)
    #=
    CASS_BATCH_TYPE_LOGGED = 0x00
    CASS_BATCH_TYPE_UNLOGGED = 0x01
    CASS_BATCH_TYPE_COUNTER = 0x02
    =#
    batch = ccall(
                (:cass_batch_new, "CASSLIBNAME"),
                Ptr{CassBatch},
                (Cuchar,),
                batch_type)
    return batch::Ptr{CassBatch}
end

function cql_future_get_prepared(future::Ptr{CassFuture})
    prep = ccall(
            (:cass_future_get_prepared, "CASSLIBNAME"),
            Ptr{CassPrepared},
            (Ptr{CassFuture},),
            future)
    return prep::Ptr{CassPrepared}
end

function cql_prepared_bind(prep::Ptr{CassPrepared})
    statement = ccall(
                    (:cass_prepared_bind, "CASSLIBNAME"),
                    Ptr{CassStatement},
                    (Ptr{CassPrepared},),
                    prep)
    return statement::Ptr{CassStatement}
end

function cql_uuid_gen_new()
    uuid_gen = ccall(
        (:cass_uuid_gen_new, "CASSLIBNAME"),
        Ptr{CassUuidGen},
        ())
    return uuid_gen::Ptr{CassUuidGen}
end

function cql_uuid_gen_free(uuid_gen::Ptr{CassUuidGen})
    ccall(
        (:cass_uuid_gen_free, "CASSLIBNAME"),
        Nothing,
        (Ptr{CassUuidGen},),
        uuid_gen)
end

function cql_uuid_gen_random(uuid_gen::Ptr{CassUuidGen})
    uuid = get_null_cass_uuid_ref()
    ccall(
        (:cass_uuid_gen_random, "CASSLIBNAME"),
        Nothing,
        (Ptr{CassUuidGen}, Ref{CassUuid}),
        uuid_gen, uuid)
    return uuid.x
end

function cql_statement_bind_uuid(statement::Ptr{CassStatement}, pos::Int, data::UUID)
    ccall(
        (:cass_statement_bind_uuid, "CASSLIBNAME"),
        Nothing,
        (Ptr{CassStatement}, Cint, CassUuid),
        statement, pos, CassUuid(data))
end

function cql_statement_bind_string(statement::Ptr{CassStatement}, pos::Int, data::String)
    ccall(
        (:cass_statement_bind_string, "CASSLIBNAME"),
        Nothing,
        (Ptr{CassStatement}, Cint, Cstring),
        statement, pos, data)
end

function cql_statement_bind_int8(statement::Ptr{CassStatement}, pos::Int, data::Int8)
    ccall(
        (:cass_statement_bind_int8, "CASSLIBNAME"),
        Nothing,
        (Ptr{CassStatement}, Cint, Cshort),
        statement, pos, data)
end

function cql_statement_bind_int16(statement::Ptr{CassStatement}, pos::Int, data::Int16)
    ccall(
        (:cass_statement_bind_int16, "CASSLIBNAME"),
        Nothing,
        (Ptr{CassStatement}, Cint, Cshort),
        statement, pos, data)
end

function cql_statement_bind_int32(statement::Ptr{CassStatement}, pos::Int, data::Int32)
    ccall(
        (:cass_statement_bind_int32, "CASSLIBNAME"),
        Nothing,
        (Ptr{CassStatement}, Cint, Cint),
        statement, pos, data)
end

function cql_statement_bind_int64(statement::Ptr{CassStatement}, pos::Int, data::Int64)
    ccall(
        (:cass_statement_bind_int64, "CASSLIBNAME"),
        Nothing,
        (Ptr{CassStatement}, Cint, Clonglong),
        statement, pos, data)
end

function cql_statement_bind_bool(statement::Ptr{CassStatement}, pos::Int, data::Bool)
    ccall(
        (:cass_statement_bind_bool, "CASSLIBNAME"),
        Nothing,
        (Ptr{CassStatement}, Cint, Cint),
        statement, pos, data)
end

function cql_statement_bind_uint32(statement::Ptr{CassStatement}, pos::Int, data::UInt32)
    ccall(
        (:cass_statement_bind_uint32, "CASSLIBNAME"),
        Nothing,
        (Ptr{CassStatement}, Cint, Cuint),
        statement, pos, data)
end

function cql_statement_bind_double(statement::Ptr{CassStatement}, pos::Int, data::Float64)
    ccall(
        (:cass_statement_bind_double, "CASSLIBNAME"),
        Nothing,
        (Ptr{CassStatement}, Cint, Cdouble),
        statement, pos, data)
end

function cql_statement_bind_float(statement::Ptr{CassStatement}, pos::Int, data::Float32)
    ccall(
        (:cass_statement_bind_float, "CASSLIBNAME"),
        Nothing,
        (Ptr{CassStatement}, Cint, Cfloat),
        statement, pos, data)
end

function cql_statement_bind_null(statement::Ptr{CassStatement}, pos::Int, ::Missing)
    ccall(
        (:cass_statement_bind_null, "CASSLIBNAME"),
        Nothing,
        (Ptr{CassStatement}, Cint),
        statement, pos)
end

function cql_batch_add_statement(batch::Ptr{CassBatch}, statement::Ptr{CassStatement})
    ccall(
        (:cass_batch_add_statement, "CASSLIBNAME"),
        Nothing,
        (Ptr{CassBatch}, Ptr{CassStatement}),
        batch, statement)
end

function cql_session_execute_batch(session::Ptr{CassSession}, batch::Ptr{CassBatch})
    future = ccall(
                (:cass_session_execute_batch, "CASSLIBNAME"),
                Ptr{CassFuture},
                (Ptr{CassSession}, Ptr{CassBatch}),
                session, batch)
    return future::Ptr{CassFuture}
end

function cql_batch_free(batch::Ptr{CassBatch})
    ccall(
        (:cass_batch_free, "CASSLIBNAME"),
        Nothing,
        (Ptr{CassBatch},),
        batch)
end

function cql_prepared_free(prep::Ptr{CassPrepared})
    ccall(
        (:cass_prepared_free, "CASSLIBNAME"),
        Nothing,
        (Ptr{CassPrepared},),
        prep)
end
