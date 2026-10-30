import { PageHeader } from "@/components/page-header"
import { Tester } from "@/components/tester"

export const dynamic = "force-dynamic"

export default function Home() {
  return (
    <main className="mx-auto w-full max-w-7xl space-y-6 px-4 py-6 sm:px-6 sm:py-10 lg:px-8">
      <PageHeader
        title="Geocoding tester"
        description="Try GEOCODING.PUBLIC forward geocoding, reverse geocoding and address normalization on Overture Maps data."
      />
      <Tester />
    </main>
  )
}
