# FDPO Fabric Current Status

Last updated: 2026-02-18

## Scope

Track current progress for assigning workspace `fdpo-access-check-ws-20260218-171328` to FDPO Fabric F2 capacity.

## Verified Context

- Tenant: `16b3c013-d300-468d-ac64-7eda0820b6d3` (`fdpo.onmicrosoft.com`)
- Subscription: `a20bc194-9787-44ee-9c7f-7c3130e651b6`
- User login confirmed in CLI as `ozgurguler@microsoft.com`

## Completed

1. Workspace created:
   - Name: `fdpo-access-check-ws-20260218-171328`
   - Workspace ID: `6bf216a8-e74a-44cc-9c0d-6f141c82d487`
2. Fabric capacity created in Azure:
   - Name: `fdpofabricf20218202320`
   - SKU: `F2`
   - Resource group: `rg-fabric`
   - Region: `UK South`
   - Provisioning state: `Succeeded`
   - State: `Active`
3. Capacity admins currently set on ARM resource:
   - `51640ebf-8904-4995-ad68-1b89a65ccf0e` (service principal)
   - `4d2ec23b-c38e-4af5-8607-4936f61d6d55` (service principal)

## Current Runtime State

- Workspace is still assigned to PPU capacity:
  - `capacityId`: `cc5bfcb0-13fc-47b7-88c0-9f4c07a4af33`
  - `capacityRegion`: `West US 3`
- `GET https://api.fabric.microsoft.com/v1/capacities` returns only:
  - `Premium Per User - Reserved` (`PP3`)
- F2 capacity is not visible in Fabric capacity list for current user identity.

## Blocking Issues

1. Current FDPO identity is B2B guest-shaped in tenant (`...#EXT#`), not accepted for Fabric capacity admin assignment in this flow.
2. Current user has no active Entra directory admin role (no transitive directory roles).
3. Attempts to add `Fabric Administrator` role fail with:
   - `Authorization_RequestDenied` / insufficient privileges
4. Attempts to create tenant-native user fail with:
   - `Insufficient privileges to complete the operation`
5. Service principal API path is not usable yet because tenant-level Fabric admin settings/permissions are not completed.

## What Is Needed To Finish

Any one of these admin-side actions unblocks assignment:

1. FDPO Global Admin or Privileged Role Admin grants `Fabric Administrator` to `ozgurguler@microsoft.com`.
2. FDPO admin performs workspace assignment directly in Fabric Admin Portal.
3. FDPO admin enables service principal usage for Fabric APIs and allows the configured app/service principal path.

After one of the above, rerun capacity visibility check and assign workspace to F2.

## Quick Verification Commands

```bash
az account show -o json
az rest --resource https://api.fabric.microsoft.com --method GET --url https://api.fabric.microsoft.com/v1/capacities -o json
az rest --resource https://api.fabric.microsoft.com --method GET --url https://api.fabric.microsoft.com/v1/workspaces/6bf216a8-e74a-44cc-9c0d-6f141c82d487 -o json
```

## Security Note

- A client secret was generated during service principal bootstrap during troubleshooting.
- Rotate/revoke prior secrets after final setup is complete.
