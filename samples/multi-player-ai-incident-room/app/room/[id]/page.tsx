import { RoomClient } from "@/components/room/room-client"

export const dynamic = "force-dynamic"

export default async function RoomPage({ params }: { params: Promise<{ id: string }> }) {
  const { id } = await params
  return <RoomClient incidentId={id} dev={process.env.NODE_ENV !== "production"} />
}
