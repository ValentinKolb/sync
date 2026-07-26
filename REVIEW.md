# Code Review — @k2b/sync/browser

## Zusammenfassung
Die Browser-Sublib ist insgesamt ordentlich strukturiert, konsistent typisiert und mit einer breiten Testsuite abgesichert. Die kleineren Bausteine wirken überwiegend solide; besonders positiv sind Timer-Cleanup im Store, die `EventLog.subscribe()`-Race-Vermeidung und mehrere gute Mutex-/Scheduler-Tests. Die größten Probleme liegen nicht im Happy Path, sondern in Semantikbrüchen gegenüber der dokumentierten API: `topic` implementiert faktisch keine Consumer Groups, `job` behandelt `heartbeat()` nicht als Verlängerung des echten Ausführungszeitlimits, `registry` signalisiert Replay-Gaps nicht, und der Publish-Workflow erzeugt kaputte Browser-Typ-Exports. Die Tests sind breit, geben an einigen kritischen Stellen aber false confidence.

## Compile & Test Status
- `bunx tsc --noEmit`: erfolgreich, Exit Code `0`.
- `bun test tests/browser/*.test.ts`: erfolgreich, `257` Tests grün, `0` Fehler, Laufzeit `81.04s`.

## Findings pro Modul

### store.ts
#### Bugs
- `src/browser/store.ts:145-149` + `src/browser/store.ts:153-159`: `LocalStorageStore` koppelt TTL-Löschung an instanzlokale Timer ohne Versions- oder Wertprüfung. Zwei `LocalStorageStore`-Instanzen mit gleichem Prefix koordiniert nichts; ein alter Timer aus Instanz A kann einen neueren Write aus Instanz B löschen. Repro: `a.set("k", 1, 50)` gefolgt von `b.set("k", 2, 500)` ergibt nach ~80ms wieder `undefined`.
- `src/browser/store.ts:167-180`: `LocalStorageStore.keys()` löscht abgelaufene Keys während numerischer `localStorage`-Iteration. Dadurch verschieben sich spätere Indizes; das nächste Element kann übersprungen werden. Repro mit zwei bereits abgelaufenen Raw-Entries lässt einen alten Key in `localStorage` zurück.

#### Code Smells
- `src/browser/store.ts:116-143` + `src/browser/store.ts:153-212`: `LocalStorageStore` behandelt weder `SecurityError` noch `QuotaExceededError`. In privaten Modi, sandboxed iframes oder blockiertem Storage können einfache `getItem`/`setItem`/`removeItem`-Aufrufe hart werfen statt kontrolliert zu degradieren.

#### Sicherheit
- Keine eigenständigen Sicherheitsfindings jenseits der Robustheit oben.

#### Sonstiges
- `MemoryStore` ist für den Single-Instance-Fall sauber umgesetzt; `src/browser/store.ts:38-44`, `src/browser/store.ts:57-63` und `src/browser/store.ts:85-90` räumen Timer zuverlässig auf.

### internal/sleep.ts
#### Bugs
- Keine materiellen Findings.

#### Code Smells
- Keine materiellen Findings.

#### Sicherheit
- Keine materiellen Findings.

#### Sonstiges
- `src/browser/internal/sleep.ts:18-31`: gute Cleanup-Logik bei Abort; Timer und Listener werden auf beiden Pfaden entfernt.

### internal/emitter.ts
#### Bugs
- Keine materiellen Findings.

#### Code Smells
- Keine materiellen Findings.

#### Sicherheit
- Keine materiellen Findings.

#### Sonstiges
- `src/browser/internal/emitter.ts:30-48`: `onceWithSignal()` räumt Subscriptions sauber auf und ist für die internen Wait-Loops brauchbar.

### internal/event-log.ts
#### Bugs
- Keine harten Funktionsfehler im normalen internen Einsatz gefunden.

#### Code Smells
- `src/browser/internal/event-log.ts:35-55`: `append()` speichert `fields` by reference und `range()` gibt dieselben Entry-Objekte zurück. Wer intern ein Entry nachträglich mutiert, kann Historie rückwirkend verfälschen. Das ist eher ein Immutability-Leak als ein akuter Produktionsbug, aber unnötig fragil.

#### Sicherheit
- Keine materiellen Findings.

#### Sonstiges
- `src/browser/internal/event-log.ts:81-103`: die Kombination aus Buffer-Drain und anschließendem `onceWithSignal()` schließt die klassische `range()`/`subscribe()`-Race sauber.

### internal/id.ts
#### Bugs
- Keine unmittelbaren Laufzeitbugs.

#### Code Smells
- `src/browser/internal/id.ts:20-26`: `simpleHash()` ist nur ein 32-Bit-djb2-Hash, wird aber zur Normalisierung langer IDs in `ratelimit` und `mutex` verwendet. Das ist als Kürzung okay, aber nicht kollisionsarm genug für Isolationsgrenzen.

#### Sicherheit
- `src/browser/internal/id.ts:20-26` zusammen mit `src/browser/ratelimit.ts:8-10` und `src/browser/mutex.ts:11-13`: kollidierende lange Identifiers/Resources können denselben Rate-Limit-Bucket oder denselben Lock-Key teilen. Das ist kein Kryptothema, aber ein realer Alias-Risk bei kontrolliert langen Schlüsseln.

#### Sonstiges
- `randomId()` und `randomHex()` nutzen korrekte Browser-APIs (`crypto.randomUUID`, `crypto.getRandomValues`).

### retry.ts
#### Bugs
- Keine harten Funktionsfehler im Kern-Backoff gesehen.

#### Code Smells
- `src/browser/retry.ts:41-52`: `isRetryableTransportError()` matcht sehr breite Substrings wie `"connection"`, `"loading"` und `"network"`. Dadurch können permanente Anwendungsfehler standardmäßig retried werden, was unnötige Latenz oder doppelte Side Effects erzeugen kann, wenn der aufrufende Code nicht streng idempotent ist.

#### Sicherheit
- Keine materiellen Findings.

#### Sonstiges
- Abort-Pfade funktionieren erwartbar; `sleepWithSignal()` wird korrekt genutzt.

### ratelimit.ts
#### Bugs
- `src/browser/ratelimit.ts:59-84`: `windowSecs` wird nicht validiert. Mit `windowSecs <= 0` entstehen `NaN`-Werte für `remaining`/`resetIn`, und der Counter wird wegen `ttlMs <= 0` effektiv ohne Ablauf gespeichert. Repro mit `windowSecs: 0` liefert `{ limited: false, remaining: NaN, resetIn: NaN }`.

#### Code Smells
- `src/browser/ratelimit.ts:57-62`: `id`, `limit` und `windowSecs` werden im Gegensatz zu anderen Browser-Modulen kaum validiert. Das fällt besonders auf, weil `ephemeral`, `registry` und `scheduler` deutlich defensiver sind.

#### Sicherheit
- In Kombination mit `simpleHash()` gilt der Collision-Hinweis aus `internal/id.ts`.

#### Sonstiges
- Das Sliding-Window-Modell an sich ist konsistent, und die Tests decken den normalen Pfad gut ab.

### mutex.ts
#### Bugs
- `src/browser/mutex.ts:70-85` in Verbindung mit `src/browser/store.ts:46-54`: nichtpositive TTLs erzeugen faktisch unendliche Locks. `acquire("r", 0)` schreibt einen Key ohne Expiry, gibt aber ein Lock mit bereits abgelaufener `expiration` zurück. Repro: erstes `acquire(..., 0)` liefert ein Lock, zweites `acquire()` bleibt `null`.

#### Code Smells
- `src/browser/mutex.ts:63-68`: `retryCount`, `retryDelay` und `defaultTtl` werden nicht validiert oder geklemmt.

#### Sicherheit
- `src/browser/mutex.ts:96-113`: positiv; Release/Extend prüfen das Owner-Token korrekt und verhindern stale-lock spoofing im Single-Tab-Modell.

#### Sonstiges
- Ansonsten klar und gut lesbar; der Code ist klein und verhält sich in den normalen Fällen sauber.

### topic.ts
#### Bugs
- `src/browser/topic.ts:183-185`, `src/browser/topic.ts:194-196`, `src/browser/topic.ts:238-243`: `reader(group)` speichert keinerlei Gruppenstatus. Jeder Reader startet mit Cursor `"0"`, und `commit()` ist immer ein No-op. Zwei Reader derselben Gruppe konsumieren deshalb dieselben Events statt load-balanced/at-least-once zu arbeiten. Repro: zwei `reader("g")` erhalten beide dasselbe Event.
- `src/browser/topic.ts:185`, `src/browser/topic.ts:188-196`: ein Reader teilt sich einen einzigen `cursor` über alle `tenantId`s. Da jede Tenant-EventLog-ID wieder bei `1` startet, kann das Lesen aus Tenant A Events aus Tenant B überspringen. Repro: ein gemeinsamer Reader sieht `tenantId:"a"`-Event `1`, danach `tenantId:"b"`-Event `1` nicht mehr.

#### Code Smells
- `src/browser/topic.ts:184`: `consumerId` wird erzeugt und nie verwendet.
- `src/browser/topic.ts:58-63`, `src/browser/topic.ts:277-300`: `TopicLiveConfig.timeoutMs` ist exportiert, wird in `live()` aber nicht verwendet.

#### Sicherheit
- Keine eigenständigen Security-Bugs; der relevante Impact ist semantische Doppelverarbeitung.

#### Sonstiges
- `src/browser/topic.ts:201-219`: die Abort-/Timeout-Kombination in `recv()` ist sauberer umgesetzt als in `ephemeral`/`registry`.

### queue.ts
#### Bugs
- `src/browser/queue.ts:403-420`: `nack()` setzt `settled = true` und löscht Delivery/Lease, bevor `delayMs` validiert wird. Bei `delayMs > maxNackDelayMs` wird geworfen, aber die Nachricht weder requeued noch delayed. Repro: nach `msg.nack({ delayMs: 200 })` bei `maxNackDelayMs: 100` ist die Nachricht dauerhaft weg (`recv()` liefert `null`).

#### Code Smells
- `src/browser/queue.ts:257-260` + `src/browser/queue.ts:272-278`: das `payloadBytes`-Limit misst nur `data`, nicht `meta` oder `orderingKey`. Repro: ein `meta.big` mit 5000 Zeichen wird trotz `payloadBytes: 20` angenommen.
- `src/browser/queue.ts:56-63` + `src/browser/queue.ts:304-466`: `consumerId` ist Teil der API, wird aber nie benutzt.
- `src/browser/queue.ts:29-33`: `ordering.mode`/`partitions` sind exportiert, im Browser-Queue-Code aber ungenutzt.

#### Sicherheit
- Das Limit-Bypass-Problem oben ist primär ein Robustheits-/Memory-Thema, kein klassisches Security-Issue.

#### Sonstiges
- `src/browser/queue.ts:393-449`: die Settlement-Guards für `ack`/`nack`/`touch` sind ansonsten gut; das verhindert doppelte Entscheidungen pro Delivery.

### ephemeral.ts
#### Bugs
- `src/browser/ephemeral.ts:487-492`: `recv()` ignoriert `timeoutMs`, sobald gleichzeitig ein `signal` übergeben wird. Das Timeout abortet einen privaten Controller, `subscribe()` lauscht aber auf `cfg.signal ?? ac.signal`. Effekt: bounded waits können unbegrenzt hängen.

#### Code Smells
- `src/browser/ephemeral.ts:181-203`: TTLs verlassen sich vollständig auf rohe `setTimeout(ttlMs)`. Sehr große TTLs oberhalb des Browser-Timer-Limits (~24.8 Tage) können früh oder unerwartet feuern.

#### Sicherheit
- Keine materiellen Findings.

#### Sonstiges
- `src/browser/ephemeral.ts:381-400`: positiv; Replay-Gaps werden erkannt und als `overflow` signalisiert. Genau diese Semantik fehlt im Browser-`registry`.

### registry.ts
#### Bugs
- `src/browser/registry.ts:116-121` und `src/browser/registry.ts:664-776`: der öffentliche Typ verspricht `overflow`, aber `reader()` prüft Replay-Gaps nie. Sobald alte Events getrimmt wurden, laufen Reader mit altem Cursor stillschweigend vom ersten verbleibenden Event weiter. Repro bei `eventMaxLen: 2` und `after: "1"` liefert ein normales `upsert`-Event statt `overflow`.
- `src/browser/registry.ts:746-764`: gleiches Timeout-Problem wie in `ephemeral`; `timeoutMs` wird effektiv ausgeschaltet, wenn `signal` gesetzt ist. Repro: `recv({ wait: true, timeoutMs: 50, signal })` bleibt >120ms pending und endet erst mit externem Abort.

#### Code Smells
- `src/browser/registry.ts:551-560` + `src/browser/registry.ts:599-607`: `get(...includeExpired)` und `list({ status: "expired" })` casten `null` auf `T`. Das ist bewusst pragmatisch, aber typsicher ist es nicht; ein Consumer, der `RegistryEntry<T>.value` blind benutzt, kann zur Laufzeit crashen.
- `src/browser/registry.ts:571-628` + `src/browser/registry.ts:664-675`: Prefix-Eingaben werden nicht normalisiert/validiert. Fehler wie `prefix: "svc"` vs. `prefix: "svc/"` schlagen nicht früh fehl und können zu zu breiten oder leeren Ergebnissen führen.
- `src/browser/registry.ts:294-332`: große TTLs hängen ebenfalls direkt an rohen Browser-Timern.

#### Sicherheit
- Keine materiellen Findings jenseits des stillen Datenverlusts durch fehlendes `overflow`.

#### Sonstiges
- `src/browser/registry.ts:286-291`: Root-/Key-/Prefix-Fanout ist sauber gelöst.

### job.ts
#### Bugs
- `src/browser/job.ts:330-347`: `ctx.heartbeat()` verlängert nur die Queue-Lease via `message.touch()`, nicht aber das echte Ausführungs-Timeout von `withTimeout(processPromise, payload.leaseMs)`. Ein Job mit `leaseMs: 100`, der sofort `heartbeat({ leaseMs: 500 })` ruft und danach 250ms arbeitet, endet trotzdem als `timed_out`. Repro bestätigt genau dieses Verhalten.

#### Code Smells
- `src/browser/job.ts:247-253`: `writeCancelledState()` plant anders als `writeFinalState()` kein Retention-Cleanup ein. In langen Browser-Sessions sammelt `stateStore` damit alte Cancelled-Jobs unbegrenzt.

#### Sicherheit
- Keine materiellen Findings.

#### Sonstiges
- `src/browser/job.ts:341-347` und `src/browser/job.ts:544-612`: positiv; Input wird sowohl bei Submit als auch beim Run validiert, und `join()` macht Final-State-Rechecks, um Event-Races zu vermeiden.

### scheduler.ts
#### Bugs
- `src/browser/scheduler.ts:460-590` zusammen mit `src/browser/scheduler.ts:703-705`: `dispatchDue()` arbeitet auf `StoredSchedule`-Objekten aus einem in-memory Snapshot weiter, auch nachdem zwischen zwei `await`s `unregister()` oder `register()` gelaufen ist. Es gibt hier keine CAS-ähnliche Revalidierung vor dem finalen Submit. Praktischer Impact: ein gerade geändertes oder gelöschtes Schedule kann noch nach altem Stand Jobs submitten.
- `src/browser/scheduler.ts:632-637` und `src/browser/scheduler.ts:767-768`: `triggerNow()` schreibt denselben `lastRunAt`-Cursor wie reguläre Dispatches. Damit kann ein manueller Trigger nach einem Reopen-Cycle verpasste planmäßige Slots verdecken, obwohl eine manuelle Ausführung semantisch nicht dasselbe ist wie ein regulärer Cron-Slot.

#### Code Smells
- Keine weiteren materiellen Code-Smells über die Bugs oben hinaus.

#### Sicherheit
- Keine materiellen Findings.

#### Sonstiges
- `src/browser/scheduler.ts:243-258` und `src/browser/scheduler.ts:413-426`: positiv; operative Limits sind vernünftig geklemmt, DLQ-Wachstum ist bounded, und die Metrics-Hooks sind defensiv.

### index.ts
#### Bugs
- Keine materiellen Findings.

#### Code Smells
- Keine materiellen Findings.

#### Sicherheit
- Keine materiellen Findings.

#### Sonstiges
- Die Barrel-Exports sind konsistent und decken die Browser-Oberfläche vollständig ab.

### Tests

#### Allgemeine Test-Qualität
- Die Suite deckt viele echte Failure-Modes ab, vor allem bei `store`, `mutex`, `queue`, `job` und `scheduler`.
- Besonders stark: `tests/browser/mutex.test.ts:298`, `tests/browser/mutex.test.ts:323`, `tests/browser/mutex.test.ts:348`, `tests/browser/store.test.ts:163`, `tests/browser/store.test.ts:202`, `tests/browser/queue.test.ts:459`, `tests/browser/scheduler.test.ts:628`, `tests/browser/scheduler.test.ts:714`.

#### Flaky-Risiken
- `tests/browser/scheduler.test.ts:765` und `tests/browser/scheduler.test.ts:821`: globale Monkey-Patches auf `Date.now` während aktive Async-Loops laufen. Das ist shared global state; bei Parallelisierung oder Cleanup-Fehlern kann es andere Tests verfälschen.
- `tests/browser/store.test.ts:246` ff.: `globalThis.localStorage` wird pro Test überschrieben. Aktuell wird sauber zurückgesetzt, aber unter Parallelisierung ist das order-dependent.
- Mehrere TTL-Tests nutzen relativ knappe Sleeps (`50ms`, `100ms`, `200ms`). Sie sind aktuell okay, verlassen sich aber auf nicht allzu starke Timer-Throttling-/CI-Jitter.

#### False-Confidence Tests
- `tests/browser/queue.test.ts:526-535`: prüft nur, dass `nack()` bei zu großem `delayMs` wirft. Nicht geprüft wird der eigentlich kritische Teil: dass die Nachricht danach recoverbar bleibt. Genau dort liegt der Bug.
- `tests/browser/jobs.test.ts:312-344`: der Heartbeat-Test beweist nur, dass ein Heartbeat-Event emittiert wird und ein kurzer Job grün endet. Er beweist nicht, dass `heartbeat()` das echte Timeout verlängert.
- `tests/browser/ephemeral.test.ts:362-388`: der Overflow-Test akzeptiert jedes beliebige Event (`expect(event).not.toBeNull()`), nicht spezifisch `overflow`.
- `tests/browser/scheduler.test.ts:518-622`: der Testname behauptet “missing handler causes dispatch skip and leadership relinquish”, der Testkörper erreicht diesen Pfad aber explizit nicht und prüft am Ende nur normale Metriken.

#### Fehlende Coverage
- `tests/browser/topic.test.ts:194-213`: es gibt nur eine Prüfung für verschiedene Consumer Groups, aber keine für zwei Reader derselben Group. Genau dort bricht die Browser-Implementierung.
- Für `registry.reader({ after })` fehlt ein Test auf Replay-Gap/Overflow; die API verspricht dieses Event, der Browser-Code liefert es nicht.
- Für `LocalStorageStore` fehlt ein Multi-Instance-Test mit gleichem Prefix und überlappenden TTLs.

### Build & Publish Pipeline
#### Findings
- `Kritisch` `.github/workflows/publish.yml:105-118` + `.github/workflows/publish.yml:138` + `tsconfig.json:29`: die npm-Exports verweisen Browser-Typen auf `./browser/*.d.ts`, aber `tsc --outDir dist` erzeugt tatsächlich `dist/src/browser/*.d.ts`. Ich habe das lokal verifiziert. Ergebnis: publiziertes Browser-JS ist nutzbar, die Browser-Subpath-Typauflösung ist kaputt.
- `package.json:8-26`: keine materiellen Findings. Die Browser-Source-Exports und Test-Skripte sind im Repo konsistent; das Release-Problem entsteht erst beim erzeugten `dist/package.json` im Workflow.

### Dokumentation
#### Findings
- `README.md:369`: “The API is identical to the server version” ist in dieser Form zu stark. `topic.reader(group)` verhält sich im Browser gerade nicht wie eine echte Consumer Group.
- `README.md:401-410`: die Empfehlung `createLocalStorageStore()` für persistente Browser-Zustände ist sinnvoll, aber die Doku erwähnt die relevanten Einschränkungen nicht: main-thread-only `localStorage`, mögliche `SecurityError`/Quota-Fehler und die fehlende Instanz-/Tab-Koordination der TTL-Timer.
- `README.md:455`: die Browser-Tests sind zwar grün, aber einzelne Kernsemantiken bleiben ungetestet; als Qualitätsindikator ist der Satz okay, als Vertrauenssignal aber etwas optimistisch.

## Bewertung

### Kritisch (muss vor Release gefixt werden)
1. `.github/workflows/publish.yml:105-118`, `.github/workflows/publish.yml:138`, `tsconfig.json:29`: Browser-Type-Exports im Publish-Artefakt sind kaputt. Impact: `@k2b/sync/browser` publiziert JS ohne auflösbare Type Declarations.
2. `src/browser/topic.ts:183-185`, `src/browser/topic.ts:194-196`, `src/browser/topic.ts:238-243`: `topic.reader(group)` implementiert keine echte Group-Semantik. Impact: doppelte Event-Verarbeitung statt at-least-once/load-balanced delivery.
3. `src/browser/queue.ts:403-420`: invalides `nack({ delayMs })` kann Nachrichten orphanen. Impact: reale Work Items gehen dauerhaft verloren.
4. `src/browser/job.ts:330-347`: `ctx.heartbeat()` verlängert nicht das echte Job-Timeout. Impact: lange Jobs timeouten trotz Heartbeat und brechen semantisch unerwartet ab.
5. `src/browser/registry.ts:116-121`, `src/browser/registry.ts:664-776`: `registry.reader()` signalisiert Replay-Gaps nicht, obwohl die API `overflow` verspricht. Impact: Reconciliation kann stillschweigend Events verlieren.

### Wichtig (sollte gefixt werden)
1. `src/browser/store.ts:145-149`, `src/browser/store.ts:153-159`: `LocalStorageStore`-Timer können neuere Writes anderer Instanzen löschen.
2. `src/browser/topic.ts:185`, `src/browser/topic.ts:188-196`: ein Topic-Reader kann durch Tenant-Wechsel Events überspringen.
3. `src/browser/ratelimit.ts:59-84`: invalide `windowSecs` erzeugen kaputte, nicht ablaufende Limiter-State.
4. `src/browser/mutex.ts:70-85`: `ttl <= 0` erzeugt immortale Locks.
5. `src/browser/ephemeral.ts:487-492` und `src/browser/registry.ts:746-764`: `timeoutMs` wirkt nicht, wenn zusätzlich `signal` gesetzt ist.
6. `src/browser/scheduler.ts:460-590`, `src/browser/scheduler.ts:703-705`: fehlende Revalidierung/CAS nach `await` kann zu Dispatch alter oder gelöschter Schedules führen.
7. `src/browser/scheduler.ts:632-637`, `src/browser/scheduler.ts:767-768`: `triggerNow()` verschiebt denselben Recovery-Cursor wie planmäßige Runs.
8. `tests/browser/scheduler.test.ts:518-622`, `tests/browser/jobs.test.ts:312-344`, `tests/browser/queue.test.ts:526-535`, `tests/browser/ephemeral.test.ts:362-388`: mehrere Tests sind grün, prüfen die kritische Semantik aber nicht wirklich.

### Vorschläge (nice to have)
1. `src/browser/store.ts:116-143`, `src/browser/store.ts:153-212`: `LocalStorageStore` defensiver gegen Storage-Exceptions machen und Browser-Kontext klarer dokumentieren.
2. `src/browser/queue.ts:257-260`, `src/browser/queue.ts:272-278`: `payloadBytes` auf das tatsächlich gespeicherte Envelope-Objekt anwenden, nicht nur auf `data`.
3. `src/browser/internal/event-log.ts:35-55`: EventLog intern immutabler machen, um versehentliche Mutation historischer Events auszuschließen.
4. `src/browser/internal/id.ts:20-26`: für lange Identifier einen stärkeren, kollisionsärmeren Hash erwägen oder die Kollisionsfolgen explizit dokumentieren.
5. `src/browser/ephemeral.ts:181-203` und `src/browser/registry.ts:294-332`: sehr große TTLs gegen Browser-Timer-Limits absichern.

### Positiv (gut gelöst)
- `src/browser/store.ts`: Timer-Cleanup bei Overwrite/Delete/Clear ist sauber und verhindert typische stale-timeout Bugs.
- `src/browser/internal/event-log.ts:81-103`: die Subscribe-Logik ist bewusst gegen verlorene Events zwischen Snapshot und Listener-Registrierung gebaut.
- `src/browser/mutex.ts:96-113`: stale-lock safety über Owner-Tokens ist für das Browser-Modell korrekt.
- `src/browser/queue.ts`: Settlement-Guards für `ack`/`nack`/`touch` sind grundsätzlich sauber.
- `src/browser/scheduler.ts`: Metriken, Limit-Clamps und bounded Dispatch-DLQ sind robust umgesetzt.
- Die Browser-Tests sind breit und finden bereits viele reale Fehlerklassen; die Schwäche liegt eher in einigen gezielten Lücken als in fehlender Gesamtinvestition.
