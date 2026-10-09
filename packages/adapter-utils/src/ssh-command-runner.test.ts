import { afterAll, beforeAll, describe, expect, it, vi } from "vitest";
import { chmodSync, mkdtempSync, realpathSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import { createSshCommandManagedRuntimeRunner } from "./ssh.js";

let fixture: string;
beforeAll(() => {
  fixture = realpathSync(mkdtempSync(path.join(tmpdir(), "fleet-ssh-test-")));
  // Replace only SSH transport. The actual command runner and shell execute.
  const shim = path.join(fixture, "ssh");
  writeFileSync(shim, `#!/bin/sh
for arg do remote_command="$arg"; done
exec /usr/bin/env -i HOME=/nonexistent PATH=/usr/bin:/bin /bin/sh -c "$remote_command"
`);
  chmodSync(shim, 0o700);
  vi.stubEnv("PATH", `${fixture}:/usr/bin:/bin`);
});
afterAll(() => {
  vi.unstubAllEnvs();
  if (fixture) rmSync(fixture, { recursive: true, force: true });
});

function run(command: string, args: string[], env?: Record<string, string>) {
  return createSshCommandManagedRuntimeRunner({ spec: {
    host: "synthetic.invalid", port: 22, username: "synthetic",
    remoteWorkspacePath: "/tmp", remoteCwd: "/tmp", privateKey: null,
    knownHosts: null, strictHostKeyChecking: true,
  } }).execute({ command, args, env, timeoutMs: 10000 });
}

describe("SSH command runner argument and environment contract", { timeout: 20000 }, () => {
  it("preserves shell command name and positional arguments as literal data", async () => {
    const result = await run("sh", ["-c", 'set -eu; printf "%s\\n" "$0" "$1" "$2"',
      "fixture-name", "space and ' quote", "$(printf WRONG); literal"]);
    expect(result.exitCode).toBe(0);
    expect(result.stdout).toBe("fixture-name\nspace and ' quote\n$(printf WRONG); literal\n");
  });
  it("honors bash semantics and the requested exit status", async () => {
    const result = await run("bash", ["-c", '[[ "$1" == "expected" ]] || exit 17; exit 23', "fixture", "expected"]);
    expect(result.exitCode).toBe(23);
  });
  it("sets quoted environment values for a non-shell executable", async () => {
    const result = await run("/usr/bin/printenv", ["FLEET_SYNTHETIC"], { FLEET_SYNTHETIC: "space ' $(printf WRONG); literal" });
    expect(result.exitCode).toBe(0);
    expect(result.stdout).toBe("space ' $(printf WRONG); literal\n");
  });
  it("sets the environment without losing a launch script's positional arguments", async () => {
    const result = await run("sh", ["-c", 'set -eu; printf "%s:%s\\n" "$FLEET_SYNTHETIC" "$1"',
      "fixture", "arg"], { FLEET_SYNTHETIC: "synthetic" });
    expect(result.exitCode).toBe(0);
    expect(result.stdout).toBe("synthetic:arg\n");
  });
});
