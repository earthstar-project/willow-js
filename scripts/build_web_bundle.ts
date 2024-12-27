import * as esbuild from "npm:esbuild";
import {
  denoPlugins,
  denoResolverPlugin,
  denoLoaderPlugin,
} from "@luca/esbuild-deno-loader";

import ts from "npm:typescript@5.7.2";

const version = Deno.args[0];

const cwd = Deno.cwd();

const result = await esbuild.build({
  plugins: [
    denoResolverPlugin({ configPath: `/${cwd}/deno.json` }),
    denoLoaderPlugin({ configPath: `/${cwd}/deno.json` }),
  ],
  entryPoints: ["./mod.browser-bundle.ts"],
  outfile: `./dist/willow${version ? `-${version}` : ""}.web.js`,
  bundle: true,
  format: "esm",
  platform: "browser",
  sourcemap: "linked",
  minify: true,
  metafile: true,
});

if (result.metafile) {
  await Deno.writeTextFile(
    "./dist/metafile.json",
    JSON.stringify(result.metafile)
  );
}

const program = ts.createProgram({
  rootNames: ["./mod.browser-bundle.ts"],
  options: {
    declaration: true,
    emitDeclarationOnly: true,
    outFile: "./dist/index.d.ts",
  },
});

program.emit();

Deno.exit(0);
