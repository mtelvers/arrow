open Configurator.V1

let () =
  main ~name:"arrow" (fun c ->
    let t = { Pkg_config.libs = []; cflags=[] } in
    let t = match Pkg_config.get c with
    | None -> t
    | Some pc -> begin
        match Pkg_config.query pc ~package:"parquet" with
        | None -> t
        | Some conf -> conf
    end in
    Flags.write_sexp "c_flags.sexp" t.cflags;
    Flags.write_sexp "c_library_flags.sexp" ("-lstdc++"::t.libs)
  )
